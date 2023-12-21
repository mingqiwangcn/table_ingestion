import os
import json
import random
import tiktoken
import numpy as np
import glob
from table_ingestion import util
import gpt
from openai import OpenAI
import uuid

class SqlOP:
    eq = '=',
    greater = '>',
    less = '<',
    between = 'between and'

class ChatGptGenerator:
    def __init__(self, prompt_file):
        self.buffer = []
        self.q_size_per_table = 20
        self.token_encoding = tiktoken.encoding_for_model(gpt.MODEL_NAME)
        self.max_caption_size = util.Max_Title_Size 
        self.max_col_size = util.Max_Col_Header_Size
        self.max_cell_size = util.Max_Cell_Size
        self.max_cols = 50
        output_size = 1000
        self.ctx_size = 4097 - output_size 
        self.init_prompt(prompt_file)
        self.init_messages()
        #set API key
        api_key = os.getenv('OPENAI_API_KEY', None)
        if api_key is None:
            raise ValueError('Need to set environment variable OPENAI_API_KEY')
        self.client = OpenAI(api_key=api_key)
        log_file = os.path.join(os.path.dirname(prompt_file), 'log.txt')
        f_log = open(log_file, 'a')
        gpt.set_logger(f_log)
        self.sql_op_lst = [SqlOP.eq, SqlOP.greater, SqlOP.less, SqlOP.between]
        
    def init_prompt(self, prompt_dir):
        file_pattern = os.path.join(prompt_dir, 'general.pmt')
        prompt_file_lst = glob.glob(file_pattern)
        self.start_prompt_lst = []
        for prompt_file in prompt_file_lst:
            with open(prompt_file) as f:
                prompt_text = f.read()
                self.start_prompt_lst.append(prompt_text)
        self.prompt_caption_tag = 'Table Caption:'
        self.prompt_cell_tag = 'Table Data:'
        self.prompt_sql_tag = '\nSQLs:'
        self.prompt_sql_tag_size = len(self.token_encoding.encode(self.prompt_sql_tag))

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

    def truncate_text(self, text, max_tokens):
        token_integers = self.token_encoding.encode(text)
        tokens = token_integers[:max_tokens]
        text = self.token_encoding.decode(tokens)
        return text, len(tokens)

    def process_caption(self, table_data):
        if table_data.get('gpt_title', None) is not None:
            return
        caption, caption_size = self.truncate_text(table_data['documentTitle'], self.max_caption_size)
        table_data['gpt_title'] = caption
        table_data['gpt_title_size'] = caption_size

    def truncate_columns(self, table_data):
        col_data = table_data['columns']
        if len(col_data) <= self.max_cols:
            return
        table_data['columns'] = col_data[:self.max_cols]
        row_data = table_data['rows']
        for row_item in row_data:
            cell_data = row_item['cells']
            row_item['cells'] = cell_data[:self.max_cols]

    def process_col_header(self, table_data):
        if table_data.get('gpt_total_col_size') is not None:
            return
        col_data = table_data['columns']
        total_size = 0
        for col_info in col_data:
            col_name = col_info['text']
            if '\t' in col_name:
                col_name = col_name.replace('\t', ' ')
            col_text, col_size = self.truncate_text(col_name, self.max_col_size)
            col_info['gpt_text'] = col_text
            col_info['gpt_size'] = col_size
            total_size += col_size
        table_data['gpt_total_col_size'] = total_size
        
    def get_col_header_prompt(self, table_data):
        col_data = table_data['columns']
        gpt_text_lst = [a['gpt_text'] for a in col_data] 
        header_prompt = '\t'.join(gpt_text_lst)
        return header_prompt, table_data['gpt_total_col_size'] 
   
    def process_row_cells(self, table_data):
        row_data = table_data['rows']
        for row_item in row_data:
            cell_lst = row_item['cells']
            for cell_info in cell_lst:
                item_text = cell_info['text']
                if '\t' in item_text:
                    item_text = item_text.replace('\t', ' ')
                gpt_text, gpt_size = self.truncate_text(item_text, self.max_cell_size)
                cell_info['gpt_text'] = gpt_text
         
    def col_data_complete(self, row_item, col_lst):
        for col in col_lst:
            cell_text = row_item['cells'][col]['gpt_text'] 
            if cell_text == '':
                return False
        return True

    def sample_sql(self, table_data, sample_size):
        row_data = table_data['rows']
        row_lst = list(range(len(row_data)))
        col_data = table_data['columns']
        col_lst = list(range(len(col_data)))
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
            col_samples = random.sample(col_lst, num_col_sample)
            
            sel_col = col_samples[0]
            sel_col_name = col_data[sel_col]['gpt_text']
            where_cols = col_samples[1:]
            where_col_names = [col_data[a]['gpt_text'] for a in where_cols]
            
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
                col_cell_text = row_item['cells'][w_col]['gpt_text']
                col_type = col_data[w_col].get('infer_type', None)
                where_part_sql += self.get_where_sql(w_col_name, col_type, col_cell_text) 
                if offset < (len(where_cols) - 1):
                    where_part_sql += ' and '
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

    def get_table_prompt(self, prompt, table_data):
        self.truncate_columns(table_data)
        #Add table caption
        self.process_caption(table_data)
        prompt += '\n' + self.prompt_caption_tag + '\n' + table_data['gpt_title']
        #Add cell data tag
        prompt += '\n' + self.prompt_cell_tag 
        #Add col headers
        self.process_col_header(table_data)
        header_prompt, header_prompt_size = self.get_col_header_prompt(table_data)
        prompt += '\n' + header_prompt
        num_tokens = self.num_meta_tokens + len(self.token_encoding.encode(prompt))
        assert num_tokens < self.ctx_size
        #Add row data
        self.process_row_cells(table_data)
        util.infer_col_type(table_data)

        sql_info_lst = self.sample_sql(table_data, sample_size=self.q_size_per_table) 
        prompt_sql_info_lst = [] 
        row_data = table_data['rows']
        for sql_offset, sql_info in enumerate(sql_info_lst):
            sql_no = sql_offset + 1
            sample_row_idx = sql_info['meta']['row'] 
            sample_row_item = row_data[sample_row_idx]
            cell_lst = sample_row_item['cells'] 
            row_prompt = '\t'.join([a['gpt_text'] for a in cell_lst])
            row_prompt = '\n' + row_prompt
            
            #also need to consider the SQL following
            sql_text = sql_info['sql']
            sql_prompt = f'\n{sql_no}. {sql_text}' 
            sql_size = len(self.token_encoding.encode(sql_prompt))
            
            row_size = len(self.token_encoding.encode(row_prompt)) + sql_size
            
            if num_tokens + row_size + self.prompt_sql_tag_size  <= self.ctx_size:
                prompt += row_prompt
                num_tokens += row_size
                sql_info['prompt'] = sql_prompt
                prompt_sql_info_lst.append(sql_info)
            else:
                break

        prompt += self.prompt_sql_tag
        for prompt_sql_info in prompt_sql_info_lst:
            prompt += prompt_sql_info['prompt']

        return prompt, prompt_sql_info_lst

    def generate_questions(self, table_data):
        prompt_name = self.start_prompt_lst[0]
        table_prompt, sql_info_lst = self.get_table_prompt(prompt_name, table_data)
        self.messages[-1]['content'] = table_prompt
        response = gpt.chat_complete(self.client, self.messages) 
        table_sql_lst = []
        out_text_lst = response.split('\n')
        for line in out_text_lst:
            offset = line.find(' | ')
            if offset < 0:
                continue
            q_no_str = (line[:offset].strip())
            if not util.is_int(q_no_str):
                continue
            q_no = int(q_no_str)
            sql_info = sql_info_lst[q_no - 1]
            question = line[offset:].strip()
            assert question[0] == '|'
            question = question[1:].strip()
            sql_info['question'] = question
            table_sql_lst.append(sql_info)
        return table_sql_lst

