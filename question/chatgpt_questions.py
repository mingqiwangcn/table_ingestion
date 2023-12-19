import os
import json
import random
import tiktoken
import numpy as np
from table2question.chatgpt import gpt
import glob

class ChatGptGenerator:
    def __init__(self, prompt_file):
        self.buffer = []
        self.q_size_per_table = 3
        self.token_encoding = tiktoken.encoding_for_model(gpt.MODEL_NAME)
        self.max_caption_size = 100
        self.max_col_size = 20
        self.max_cell_size = 50
        output_size = 1000
        self.ctx_size = 4097 - output_size 
        self.init_prompt(prompt_file)
        self.init_messages()
        #set API key
        api_key = os.getenv('OPENAI_API_KEY', None)
        if api_key is None:
            raise ValueError('Need to set environment variable OPENAI_API_KEY')
        gpt.set_key(api_key)
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
        self.prompt_cell_tag = 'Cell Data:'

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
    
    def get_table_prompt(self, table_data):
        #Sample a start prompt
        prompt = random.sample(self.start_prompt_lst, 1)[0]
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
        row_data = table_data['rows']
        row_idx_arr = np.arange(len(row_data)).tolist()
        used_row_dict = {}
        while True:
            if len(used_row_dict) == len(row_data):
                break
            sample_row_idx = random.sample(row_idx_arr, 1)[0]
            if sample_row_idx in used_row_dict:
                continue
            #row_total_size = 0
            sample_row_item = row_data[sample_row_idx]
            cell_lst = sample_row_item['cells']
            for cell_info in cell_lst:
                item_text = cell_info['text']
                if '\t' in item_text:
                    item_text = item_text.replace('\t', ' ')
                gpt_text, gpt_size = self.truncate_text(item_text, self.max_cell_size)
                cell_info['gpt_text'] = gpt_text
                #cell_info['gpt_size'] = gpt_size
                #row_total_size += gpt_size
            
            row_prompt = '\t'.join([a['gpt_text'] for a in cell_lst])
            row_prompt = '\n' + row_prompt
            row_size = len(self.token_encoding.encode(row_prompt))
            if num_tokens + row_size <= self.ctx_size:
                prompt += row_prompt
                num_tokens += row_size
                used_row_dict[sample_row_idx] = True
            else:
                break
        return prompt

    def generate_questions(self, table_lst, batch_question_size):
        q_cnt = 0
        while q_cnt < batch_question_size:
            table_data = table_lst[1]  #random.sample(table_lst, 1)[0]
            prompt = self.get_table_prompt(table_data)
            self.messages[-1]['content'] = prompt
            response = gpt.chat_complete(self.messages) 
            q_cnt += 4
             


