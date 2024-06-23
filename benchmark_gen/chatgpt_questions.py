import os
import json
import random
import tiktoken
import numpy as np
import glob
import util
import gpt
from openai import OpenAI
import uuid
from datetime import datetime

class SqlOP:
    eq = '=',
    greater = '>',
    less = '<',
    between = 'between and'

class ChatGptGenerator:
    def __init__(self, prompt_dir):
        self.buffer = []
        self.q_size_per_table = 10
        self.prompt_dir = prompt_dir
        self.token_encoding = tiktoken.encoding_for_model(gpt.MODEL_NAME)
        self.table_sep = ' | '
        output_size = 1000
        self.ctx_size = 4097 - output_size 
        self.init_prompt_tags()
        self.init_messages()
        #set API key
        api_key = os.getenv('OPENAI_API_KEY', None)
        if api_key is None:
            raise ValueError('Need to set environment variable OPENAI_API_KEY')
        self.client = OpenAI(api_key=api_key)

        now_t = str(datetime.now())
        log_name = 'log_' + '_'.join(now_t.split()) + '.txt'
        log_file = os.path.join(prompt_dir, log_name)
        f_log = open(log_file, 'a')
        gpt.set_logger(f_log)
        self.sql_op_lst = [SqlOP.eq, SqlOP.greater, SqlOP.less, SqlOP.between]
        
    def init_prompt_tags(self):
        self.prompt_caption_tag = 'Table Caption:'
        self.prompt_cell_tag = 'Table Data:'
        self.prompt_sql_tag = '\nSQLs:'
        
    def read_prompt(self, name):
        prompt_file = os.path.join(self.prompt_dir, name + '.pmt')
        with open(prompt_file) as f:
                prompt_text = f.read()
        return prompt_text

    def init_messages(self):
        self.messages = [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": None},
        ]
        #This is from openai cookbook on how to count tokens for chat completions API calls
        #https://cookbook.openai.com/examples/how_to_count_tokens_with_tiktoken
        tokens_per_message = 3
        num_tokens = 0
        for msg in self.messages:
            num_tokens += tokens_per_message
            for key, value in msg.items():
                if value is None:
                    continue
                num_tokens += len(self.token_encoding.encode(value))
    
        num_tokens += 3  # every reply is primed with <|start|>assistant<|message|>
        self.num_meta_tokens = num_tokens
    
    def get_prompt_cols(self, table_data):
        prompt_cols = table_data['prompt_cols']
        return prompt_cols
    
    def get_prompt_rows(self, table_data):
        prompt_rows = table_data['prompt_rows']
        return prompt_rows

    def get_col_header_prompt(self, table_data):
        col_data = table_data['columns']
        prompt_cols = self.get_prompt_cols(table_data)
        text_lst = [col_data[col]['text'] for col in prompt_cols] 
        header_prompt = self.table_sep.join(text_lst)
        return header_prompt
            
    def col_data_complete(self, row_item, col_lst):
        for col in col_lst:
            cell_text = row_item['cells'][col]['text'] 
            if cell_text == '':
                return False
        return True

    def use_table_caption(self):
        return False # not use caption for test data to make it harder for retrieval

    def sample_sql(self, table_data, sample_size):
        table_caption = table_data['documentTitle'].strip()
        row_data = table_data['rows']
        row_lst = self.get_prompt_rows(table_data)
        col_data = table_data['columns']
        col_lst = self.get_prompt_cols(table_data)
        aggr_op_lst_general = [None, 'count']
        aggr_op_lst_numeric = [None, 'count', 'max', 'min', 'avg', 'sum']
        num_sql_col_lst = [2, 3]
        if max(num_sql_col_lst) > len(col_lst):
            num_sql_col_lst = [2]
        sql_info_lst = []
        max_try = sample_size * 100
        num_try = 0
        while (len(sql_info_lst) < sample_size):
            if num_try > max_try:
                break
            num_try += 1
            num_col_sample = random.sample(num_sql_col_lst, 1)[0]
            col_samples = random.sample(col_lst, min(num_col_sample, len(col_lst)))            
            sel_col = col_samples[0]
            sel_col_name = col_data[sel_col]['text']
            where_cols = col_samples[1:]
            where_col_names = [col_data[a]['text'] for a in where_cols]            
            row = random.sample(row_lst, 1)[0]
            row_item = row_data[row]
            if not self.col_data_complete(row_item, where_cols):
                continue
            select_part_sql = f'select {sel_col_name} '
            sel_infer_type = col_data[sel_col].get('infer_type', None)
            if sel_infer_type in (util.CellDataType.INT, util.CellDataType.FLOAT):
                aggr_op = random.sample(aggr_op_lst_numeric, 1)[0]
            else:
                aggr_op = random.sample(aggr_op_lst_general, 1)[0]
            if aggr_op is not None:
               select_part_sql = f'select {aggr_op}({sel_col_name}) '

            where_part_sql = ''
            cond_info_lst = []
            for offset, w_col in enumerate(where_cols):
                w_col_name = where_col_names[offset]
                col_cell_text = row_item['cells'][w_col]['text']
                col_type = col_data[w_col].get('infer_type', None)
                cond_sql, cond_info = self.get_where_sql(w_col_name, col_type, col_cell_text)
                cond_info['col'] = w_col
                cond_info_lst.append(cond_info)
                where_part_sql += cond_sql
                 
                if offset < (len(where_cols) - 1):
                    where_part_sql += ' and '
            
            if table_caption != '' and self.use_table_caption():
                 where_part_sql += ' and Table Context = ' + table_caption

            sql = select_part_sql + ' where ' + where_part_sql
            sel_info = {'col':sel_col, 'col_name':sel_col_name}
            meta = {
                'table_id':table_data['tableId'],
                'title':table_data['documentTitle'],
                'row':row,
                'sel_info':sel_info,
                'cond_info':cond_info_lst
            }
            sql_info = {'id':str(uuid.uuid4()), 'sql':sql, 'meta':meta}
            sql_info_lst.append(sql_info)
        
        return sql_info_lst

    def get_where_sql(self, col_name, col_type, cell_text):
        threshold = None
        if util.is_float(cell_text) and (col_type in [util.CellDataType.INT, util.CellDataType.FLOAT]):
            op = random.sample(self.sql_op_lst, 1)[0] 
            cell_value = float(cell_text)
            threshold_int = random.sample(list(range(10)), 1)[0]
            threshold_float = threshold_int / 10
            if col_type == util.CellDataType.INT:
                threshold = threshold_int
            else:
                threshold = threshold_float
            if op == SqlOP.greater:
                rel_value = cell_value - threshold
                where_sql = f'{col_name} {op} {threshold}'
            elif op == SqlOP.less:
                ref_value = cell_value + threshold
                where_sql = f'{col_name} {op} {threshold}'
            elif op == SqlOP.between:
                ref_value_1 = cell_value - threshold
                ref_value_2 = cell_value + threshold
                where_sql = f'{col_name} between {ref_value_1} and {ref_value_2}'
            else:
                where_sql = f'{col_name} {op} {cell_value}'
        else:
            where_sql = f"{col_name} = '{cell_text}'"
            op = SqlOP.eq
        
        cond_info = {'col':None, 'col_name':col_name, 'op':op,  
                     'cell_text':cell_text, 'threshold':threshold}
        return where_sql, cond_info
    
    def sample_prompt_data(self, table_data):
        col_data = table_data['columns']
        col_lst = list(range(len(col_data)))
        M = 50
        if len(col_lst) > M:
            Num_First = 5
            sample_cols = col_lst[:Num_First] + random.sample(col_lst[Num_First:], M - Num_First)
        else:
            sample_cols = col_lst
        table_data['prompt_cols'] = sample_cols
        row_lst = list(range(len(table_data['rows'])))
        N = 10
        if len(row_lst) > N:
            sample_rows = random.sample(row_lst, N)
        else:
            sample_rows = row_lst
        table_data['prompt_rows'] = sample_rows

    def get_row_prompts(self, table_data):
        prompt_lst = []
        row_data = table_data['rows']
        prompt_cols = self.get_prompt_cols(table_data)
        prompt_rows = self.get_prompt_rows(table_data)
        for row in prompt_rows:
            row_item = row_data[row]
            cell_lst = row_item['cells'] 
            row_prompt = '\n' + self.table_sep.join([cell_lst[a]['text'] for a in prompt_cols])
            prompt_lst.append(row_prompt)
        return prompt_lst

    def select_column_cells(self, table_data):
        item_lst = []
        prompt_cols = self.get_prompt_cols(table_data)
        col_date = table_data['columns']
        seq = 0
        aggr_op_lst = ['count', 'max', 'min', 'avg', 'sum']
        for _ in range(self.q_size_per_table):
            item_info = {}
            seq += 1
            num_query_cols = random.sample([1,2], 1)[0]
            query_col_lst = random.sample(prompt_cols, num_query_cols)
            query_col_names = [col_date[a]['text'] for a in query_col_lst]
            
            item_info['query_cols'] = query_col_names

            other_cols = [a for a in prompt_cols if a not in query_col_lst]
            M = min(random.sample([0, 1, 2], 1)[0], len(other_cols))
            if M > 0:
                col_data = table_data['columns']
                use_filter = random.sample([0, 1], 1)[0]
                filter_columns = random.sample(other_cols, M)
                if use_filter:
                    filter_info_lst = []
                    row_data = table_data['rows']
                    prompt_rows = self.get_prompt_rows(table_data)
                    sample_row = random.sample(prompt_rows, 1)[0]
                    row_cells = row_data[sample_row]['cells']
                    for col in filter_columns:
                        col_name = col_data[col]['text'] 
                        cell_text = row_cells[col]['text']
                        filter_info = {'column':col_name, 'value':cell_text, 'row':sample_row}
                        filter_info_lst.append(filter_info)
                    item_info['filters'] = filter_info_lst
            
            use_aggr = random.sample([0, 1], 1)[0]
            if use_aggr:
                aggr_op = random.sample(aggr_op_lst, 1)[0]
                item_info['query_aggr'] = aggr_op
            item_lst.append(item_info)
        return item_lst

    def get_sql_prompts(self, table_data):
        util.infer_col_type(table_data,
                            infer_cols=self.get_prompt_cols(table_data), 
                            infer_rows=self.get_prompt_rows(table_data)
                            )
        sql_info_lst = self.sample_sql(table_data, sample_size=self.q_size_per_table) 
        prompt_sql_info_lst = [] # may exceed size limit, so use another list
        row_data = table_data['rows']
        prompt_cols = self.get_prompt_cols(table_data)
        for sql_offset, sql_info in enumerate(sql_info_lst):
            sql_no = sql_offset + 1
            sql_text = sql_info['sql']
            sql_prompt = f'\n{sql_no} {sql_text}' 
            sql_info['sql_prompt'] = sql_prompt
            prompt_sql_info_lst.append(sql_info)
        
        return prompt_sql_info_lst

    def get_paraphrase_columns(self, item_lst):
        col_set = set()
        for item_info in item_lst:
            for col_name in item_info['query_cols']:
                col_set.add(col_name)
            filters = item_info.get('filters', [])
            for filter_info in filters:
                col_set.add(filter_info['column'])
        return list(col_set)

    def prompt_table_data(self, table_data):
        #Add table caption
        prompt = self.prompt_caption_tag + '\n' + table_data['documentTitle']
        #Add cell data tag
        prompt += '\n' + self.prompt_cell_tag
        header_prompt = self.get_col_header_prompt(table_data)
        prompt += '\n' + header_prompt
        row_prompt_lst = self.get_row_prompts(table_data)
        for row_prompt in row_prompt_lst:
            prompt += row_prompt
        return prompt

    def prompt_sql_to_question(self, table_data):
        self.sample_prompt_data(table_data)
        table_prompt = self.prompt_table_data(table_data)
        sql2quest_prompt = self.read_prompt('sql2question')
        sql2quest_prompt += '\n' + table_prompt
        sql2quest_prompt += self.prompt_sql_tag
        sql_info_lst = self.get_sql_prompts(table_data)
        for sql_info in sql_info_lst:
            sql2quest_prompt += sql_info['sql_prompt']
        return sql2quest_prompt, table_prompt, sql_info_lst

    def generate_questions(self, table_data): 
        sql2quest_prompt, table_prompt, sql_info_lst = self.prompt_sql_to_question(table_data)
        self.sql_to_question(sql2quest_prompt, sql_info_lst)
        
        sql_info_file = os.path.join(self.prompt_dir, 'sql_info.jsonl')
        
        copied_seq_no = 0
        copied_sql_info_lst = []
        
        for sql_info in sql_info_lst:
            question = sql_info['question']
            copied_col_lst, copied_cell_lst = self.check_copy_text(sql_info)
            copied_text_lst = copied_col_lst + copied_cell_lst
            if len(copied_text_lst) == 0:
                continue
            copied_seq_no += 1
            no_copy_prompt = f'\n{copied_seq_no}. rewrite question ` {question} ` by replacing '
            for copied_text in copied_text_lst:
                no_copy_prompt += copied_text + ' ,'
            no_copy_prompt = no_copy_prompt[:-1] + ' with other synonyms.'
            sql_info['no_copy_prompt'] = no_copy_prompt
            copied_sql_info_lst.append(sql_info)
        
        if len(copied_sql_info_lst) > 0:
            self.rewrite_question_copied_text(table_prompt, copied_sql_info_lst)
        
        with open(sql_info_file, 'w') as f_o:
            for sql_info in sql_info_lst:
                f_o.write(json.dumps(sql_info) + '\n')
        
        return []
    
    def rewrite_question_copied_text(self, table_prompt, copied_sql_info_lst):
        prompt = self.read_prompt('no_copy_text')
        prompt += '\n' + table_prompt
        for sql_info in copied_sql_info_lst:
            no_copy_prompt = sql_info['no_copy_prompt']
            prompt += no_copy_prompt
        
        self.messages[-1]['content'] = prompt
        response = gpt.chat_complete(self.client, self.messages)
        out_text_lst = response.split('\n')
        for offset, line in enumerate(out_text_lst):
            pos = line.find('.')
            if pos < 0:
                continue
            q_no = int(line[:pos])
            if q_no != (offset + 1):
                continue
            
            q_text = line[pos+1:]
            sql_info = copied_sql_info_lst[offset]
            sql_info['question_no_copy'] = q_text
        
    def check_copy_text(self, sql_info):
        question = sql_info['question']
        meta_info = sql_info['meta']
        sel_col_name = meta_info['sel_info']['col_name']
        cond_info_lst = meta_info['cond_info']
        cond_col_name_lst = [a['col_name'] for a in cond_info_lst]
        col_name_lst = [sel_col_name] + cond_col_name_lst

        question_norm = util.norm_text(question)
        copied_col_lst = []
        for col_name in col_name_lst:
            col_name_norm = util.norm_text(col_name) 
            if col_name_norm in question_norm:
                copied_col_lst.append(col_name)
        
        copied_cell_lst = []
        cell_value_lst = [a['cell_text'] for a in cond_info_lst if a['op'] == '=']
        for cell_value in cell_value_lst:
            word_lst = cell_value.split()
            if len(word_lst) >= 2:
                cell_value_norm = util.norm_text(cell_value)
                if cell_value_norm in question_norm:
                    copied_cell_lst.append(cell_value)
        
        return copied_col_lst, copied_cell_lst

    def sql_to_question(self, sql2quest_prompt, sql_info_lst):
        self.messages[-1]['content'] = sql2quest_prompt
        response = gpt.chat_complete(self.client, self.messages)
        out_text_lst = response.split('\n')
        tag = 'Paraphrased(Begin Tag):'
        for offset, line in enumerate(out_text_lst):
            quest_pos = line.find(tag)
            if quest_pos < 0:
                continue
            q_no_pos = line.find('|')
            q_no = int(line[:q_no_pos])
            if q_no != (offset + 1):
                continue
            pos = quest_pos + len(tag)
            question = line[pos:].strip()
            sql_info = sql_info_lst[offset]
            sql_info['question'] = question
