import os
import argparse
from tqdm import tqdm
import json
from chatgpt_questions import ChatGptGenerator
import pandas as pd

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', type=str, required=True)
    args = parser.parse_args()
    return args

def read_tables(args):
    data_file = '../../data/%s/tables/tables.jsonl' % args.dataset
    table_lst = []
    with open(data_file) as f:
        for line in f:
            table_data = json.loads(line)
            if len(table_data['rows']) == 0:
                continue
            yield table_data

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
    table_iterator = read_tables(args)
    generator = ChatGptGenerator('./prompt')
    table_seq_no = 0
   
    state_file = os.path.join(out_dir, 'state.json')
    state = read_state(state_file)
    if state is None:
        state = {'seq_no':None, 'id':None}

    start_seq_no = 1
    if state['seq_no'] is not None:
        start_seq_no = state['seq_no']
    
    for table_sql_lst in generator.generate_questions(table_iterator):
        table_seq_no += 1
        if len(table_sql_lst) == 0:
            continue
        if table_seq_no < start_seq_no:
            continue
        table_id = table_sql_lst[0]['meta']['table_id']
        print(f'No.{table_seq_no} id={table_id}')
        out_sql_info_file = os.path.join(out_dir, f'query_{table_seq_no}_{table_id}.jsonl')
        with open(out_sql_info_file, 'w') as f_o_sql:
            for sql_info in table_sql_lst:
                f_o_sql.write(json.dumps(sql_info) + '\n')
        state['seq_no'] = table_seq_no
        state['id'] = table_id
        write_state(state_file, state)


if __name__ == '__main__':
    main()
