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
        self.prompt_method = 'template_2'
        self.token_encoding = tiktoken.encoding_for_model(gpt.MODEL_NAME)
        #self.max_caption_size = util.Max_Title_Size 
        #self.max_col_size = util.Max_Col_Header_Size
        #self.max_cell_size = util.Max_Cell_Size
        #self.max_cols = 50
        output_size = 1000
        self.ctx_size = 4097 - output_size 
        self.init_prompt(prompt_dir)
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
        
    def init_prompt(self, prompt_dir):
        file_pattern = os.path.join(prompt_dir, f'{self.prompt_method}.pmt')
        prompt_file_lst = glob.glob(file_pattern)
        self.start_prompt_lst = []
        for prompt_file in prompt_file_lst:
            with open(prompt_file) as f:
                prompt_text = f.read()
                self.start_prompt_lst.append(prompt_text)
        self.prompt_caption_tag = 'Table Caption:'
        self.prompt_cell_tag = 'Table Data:'
        self.prompt_sql_tag = '\nSQLs:'
        self.prompt_col_row_tag = '\nColumns and Rows:'
        #self.prompt_sql_tag_size = len(self.token_encoding.encode(self.prompt_sql_tag))

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
        header_prompt = '\t'.join(text_lst)
        return header_prompt
            
    def col_data_complete(self, row_item, col_lst):
        for col in col_lst:
            cell_text = row_item['cells'][col]['text'] 
            if cell_text == '':
                return False
        return True

    def use_table_caption(self):
       return random.sample([0,1],1)[0]

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
            for offset, w_col in enumerate(where_cols):
                w_col_name = where_col_names[offset]
                col_cell_text = row_item['cells'][w_col]['text']
                col_type = col_data[w_col].get('infer_type', None)
                where_part_sql += self.get_where_sql(w_col_name, col_type, col_cell_text) 
                if offset < (len(where_cols) - 1):
                    where_part_sql += ' and '
            
            if table_caption != '' and self.use_table_caption():
                 where_part_sql += ' and Table Context = ' + table_caption

            sql = select_part_sql + ' where ' + where_part_sql
            meta = {
                'table_id':table_data['tableId'],
                'title':table_data['documentTitle'],
                'row':row,
                'sel_col':sel_col,
                'sel_col_name':sel_col_name,
                'where_cols':where_cols,
                'where_col_names':where_col_names
            }
            sql_info = {'id':str(uuid.uuid4()), 'sql':sql, 'meta':meta}
            sql_info_lst.append(sql_info)
        
        return sql_info_lst

    def get_where_sql(self, col_name, col_type, cell_text):
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
        return where_sql
    
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
            row_prompt = '\n' + '\t'.join([cell_lst[a]['text'] for a in prompt_cols])
            prompt_lst.append(row_prompt)
        return prompt_lst

    def get_selected_prompts(self, table_data):
        item_lst = []
        prompt_cols = self.get_prompt_cols(table_data)
        col_date = table_data['columns']
        seq = 0
        for _ in range(self.q_size_per_table):
            seq += 1
            query_col = random.sample(prompt_cols, 1)[0]
            col_name = col_date[query_col]['text']
            item_prompt = f'\n{seq}. query column : {col_name}'

            other_cols = [a for a in prompt_cols if a != query_col]
            M = min(random.sample([0, 1, 2], 1)[0], len(other_cols))
            if M > 0:
                item_prompt += ' ; '
                col_data = table_data['columns']
                use_specific_row = random.sample([0, 1], 1)[0]
                filter_columns = random.sample(other_cols, M)
                if use_specific_row:
                    row_data = table_data['rows']
                    prompt_rows = self.get_prompt_rows(table_data)
                    sample_row = random.sample(prompt_rows, 1)[0]
                    row_cells = row_data[sample_row]['cells']
                    
                    for col in filter_columns:
                        col_name = col_data[col]['text'] 
                        cell_text = row_cells[col]['text']
                        filter_prompt = f'filter column/value : f{col_name}/f{cell_text} ; '
                        item_prompt += ' ' + filter_prompt
                else:
                    for col in filter_columns:
                        col_name = col_data[col]['text']
                        item_prompt += f'filter column:{col_name}'
            
            
            use_aggr = random.sample([0, 1], 1)[0]
            if use_aggr:
                item_prompt += ' ; use aggregation for query column'
            else:
                item_prompt += ' ; find specific values for query column'
            item_lst.append(item_prompt)
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

    def get_table_prompt(self, start_prompt, table_data):
        self.sample_prompt_data(table_data)
        prompt = start_prompt
        #Add table caption
        prompt += '\n' + self.prompt_caption_tag + '\n' + table_data['documentTitle']
        #Add cell data tag
        prompt += '\n' + self.prompt_cell_tag
        #Add col headers
        
        header_prompt = self.get_col_header_prompt(table_data)
        prompt += '\n' + header_prompt
        row_prompt_lst = self.get_row_prompts(table_data)
        for row_prompt in row_prompt_lst:
            prompt += row_prompt

        item_lst = self.get_selected_prompts(table_data)
        for item in item_lst:
            prompt += item
        
        prompt += '\noutput questions:\n'
        '''
        prompt += self.prompt_sql_tag
        sql_info_lst = self.get_sql_prompts(table_data)
        for sql_info in sql_info_lst:
            prompt += sql_info['sql_prompt']
        return prompt, sql_info_lst
        '''
        return prompt

    def generate_questions(self, table_data):
        start_prompt = self.start_prompt_lst[0]
        table_prompt = self.get_table_prompt(start_prompt, table_data)
        self.messages[-1]['content'] = table_prompt
        response = gpt.chat_complete(self.client, self.messages)
        question_lst = []
        out_text_lst = response.split('\n')
        for line in out_text_lst:
            question_lst.append(line)
        return question_lst
