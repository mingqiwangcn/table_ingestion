import os
import json
import random
import tiktoken
import numpy as np
import glob
from table_ingestion import util
import gpt
from openai import OpenAI

class ChatGptGenerator:
    def __init__(self, prompt_file):
        self.buffer = []
        self.q_size_per_table = 20
        self.token_encoding = tiktoken.encoding_for_model(gpt.MODEL_NAME)
        self.max_caption_size = util.Max_Title_Size 
        self.max_col_size = util.Max_Col_Header_Size
        self.max_cell_size = util.Max_Cell_Size
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

    def init_prompt(self, prompt_dir):
        file_pattern = os.path.join(prompt_dir, '*.pmt')
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
         

    def sample_sql(self, table_data, sample_size):
        row_data = table_data['rows']
        row_lst = list(range(len(row_data)))
        col_data = table_data['columns']
        col_lst = list(range(len(col_data)))
        aggr_op_lst_general = [None, 'count']
        aggr_op_lst_numeric = [None, 'count', 'max', 'min', 'avg', 'sum']
        num_sql_col_lst = [2, 3]
        if max(num_sql_col_lst) < 3:
            num_sql_col_lst = [2]
        sql_info_lst = []
        while (len(sql_info_lst) < sample_size):
            num_col_sample = random.sample(num_sql_col_lst, 1)[0]
            col_samples = random.sample(col_lst, num_col_sample)
            
            sel_col = col_samples[0]
            sel_col_name = col_data[sel_col]['gpt_text']
            where_cols = col_samples[1:]
            where_col_names = [col_data[a]['gpt_text'] for a in where_cols]
            
            row = random.sample(row_lst, 1)[0]
            row_item = row_data[row]

            select_part_sql = f'select {sel_col_name} '
            infer_type = col_data[sel_col].get('infer_type', None)
            if infer_type in (util.CellDataType.INT, util.CellDataType.FLOAT):
                aggr_op = random.sample(aggr_op_lst_numeric, 1)[0]
            else:
                aggr_op = random.sample(aggr_op_lst_general, 1)[0]

            if aggr_op is not None:
               select_part_sql = f'select {aggr_op}({sel_col_name}) '   
            
            where_part_sql = ''
            
            for offset, w_col in enumerate(where_cols):
                w_col_name = where_col_names[offset]
                col_cell_text = row_item['cells'][w_col]['gpt_text']
                where_part_sql += f"{w_col_name} = '{col_cell_text}'"
                if offset < (len(where_cols) - 1):
                    where_part_sql += ' and '
            
            sql = select_part_sql + ' where ' + where_part_sql
            
            meta = {
                'table_id':table_data['tableId'],
                'row':row,
                'sel_col':sel_col,
                'sel_col_name':sel_col_name,
                'where_cols':where_cols,
                'where_col_names':where_col_names
            }
            
            sql_info = {'sql':sql, 'meta':meta}
            sql_info_lst.append(sql_info)
        
        return sql_info_lst

    def get_table_prompt(self, prompt, table_data):
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
                raise ValueError('too many tokens') 
        
        prompt += self.prompt_sql_tag
        for prompt_sql_info in prompt_sql_info_lst:
            prompt += prompt_sql_info['prompt']

        return prompt, prompt_sql_info_lst

    def generate_questions(self, table_iterator):
        for table_data in table_iterator: 
            for prompt_name in self.start_prompt_lst:
                table_prompt, sql_info_lst = self.get_table_prompt(prompt_name, table_data)
                self.messages[-1]['content'] = table_prompt
                response = gpt.chat_complete(self.client, self.messages) 


