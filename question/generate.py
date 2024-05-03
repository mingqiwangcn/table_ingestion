import os
import argparse
from tqdm import tqdm
import json
from chatgpt_questions import ChatGptGenerator
import pandas as pd
import glob

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', type=str)
    parser.add_argument('--strategy', type=str)
    args = parser.parse_args()
    return args

def read_tables(args):
    file_pattern = os.path.join(args.sample_dir, 'tables/*.jsonl')
    table_file_lst = glob.glob(file_pattern)
    table_lst = []
    for table_file in tqdm(table_file_lst):
        with open(table_file) as f:
            for line in f:
                table_data = json.loads(line)
                table_lst.append(table_data)
    return table_lst

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
    out_dir = f'./output/{args.dataset}/{args.strategy}'
    if not os.path.isdir(out_dir):
        os.makedirs(out_dir)
    work_dir = os.path.dirname(os.getcwd())
    args.work_dir = work_dir
    args.sample_dir = os.path.join(work_dir, 'output', args.dataset, args.strategy, 'samples')
    generator = ChatGptGenerator('./prompt')
    
    table_lst = read_tables(args)
    ou_question_file = os.path.join(out_dir, 'questions.jsonl')
    with open(ou_question_file, 'w') as f_o:
        for table_data in tqdm(table_lst, desc='generate questions'):
            question_lst = generator.generate_questions(table_data)
            for question in question_lst:
                q_info = {
                    'table_number':table_data['table_number'],
                    'question':question
                }
                f_o.write(json.dumps(q_info) + '\n')
    
if __name__ == '__main__':
    main()
