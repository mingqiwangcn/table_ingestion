import os
import argparse
from tqdm import tqdm
import json
from chatgpt_questions import ChatGptGenerator
import pandas as pd
import random
import glob
import transformers
import util

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--work_dir', type=str)
    parser.add_argument('--dataset', type=str)
    args = parser.parse_args()
    return args

def read_tables(args):
    tokenizer = transformers.BertTokenizerFast.from_pretrained('bert-base-uncased')
    file_pattern = os.path.join(args.work_dir, 'data', args.dataset, 'tables/*.jsonl')
    table_file_lst = glob.glob(file_pattern)
    table_dict = {}
    for table_file in tqdm(table_file_lst):
        with open(table_file) as f:
            for line in f:
                table_data = json.loads(line)
                if table_data['documentTitle'] != 'irac classification':
                    continue
                util.preprocess_schema(tokenizer, table_data)
                table_id = table_data['tableId']
                table_dict[table_id] = table_data
    return table_dict

def read_state(state_file):
    if not os.path.isfile(state_file):
        return None
    with open(state_file) as f:
        state = json.load(f)
    return state

def write_state(state_file, state):
    with open(state_file, 'w') as f_o:
        f_o.write(json.dumps(state))

def main():
    args = get_args()
    out_dir = f'./output/{args.dataset}'
    if not os.path.isdir(out_dir):
        os.makedirs(out_dir)
    out_question_file = os.path.join(out_dir, 'questions.jsonl')
    if os.path.isfile(out_question_file):
        print(f'{out_question_file} already exists')
        return
    generator = ChatGptGenerator('./prompt')
    
    table_dict = read_tables(args)
    table_id_lst = list(table_dict.keys())
    NUM_MAX = 2000
    num_questions = 0
    pbar = tqdm(total=NUM_MAX)
    with open(out_question_file, 'w') as f_o:
        while True:
            table_id = random.sample(table_id_lst, 1)[0]
            table_data = table_dict[table_id]
            question_lst = generator.generate_questions(table_data)
            for q_info in question_lst:
                #q_info['file_name'] = table_data['tableId']
                f_o.write(json.dumps(q_info) + '\n')
            
            pbar.update(len(question_lst))
            num_questions += len(question_lst)
            if num_questions >= NUM_MAX:
                break

if __name__ == '__main__':
    main()
