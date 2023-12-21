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
    out_col_names = ['id', 'table_id', 'table_title', 'question', 
                     'answer_row', 'answer_row(grid)', 
                     'answer_col', 'answer_col(grid)', 
                     'where cols', 'where_cols(grid)']
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

        out_data = []
        table_id = table_sql_lst[0]['meta']['table_id']
        print(f'No.{table_seq_no} id={table_id}')
        out_sql_info_file = os.path.join(out_dir, f'sql_{table_seq_no}_{table_id}.jsonl')
        f_o_sql = open(out_sql_info_file, 'w')
        for sql_info in table_sql_lst:
            f_o_sql.write(json.dumps(sql_info) + '\n')
            meta_info = sql_info['meta']
            out_item = [
                        sql_info['id'],
                        meta_info['table_id'],
                        meta_info['title'],
                        sql_info['question'],
                        meta_info['row'],
                        meta_info['row'] + 2,
                        meta_info['sel_col'],
                        numer_to_letter(meta_info['sel_col'] + 1),
                        meta_info['where_cols'],
                        [numer_to_letter(a+1) for a in meta_info['where_cols']]
            ] 
            out_data.append(out_item)
        
        f_o_sql.close()
        out_file = os.path.join(out_dir, f'question_{table_seq_no}_{table_id}.csv')
        df = pd.DataFrame(data=out_data, columns=out_col_names)
        df.to_csv(out_file)
        
        state['seq_no'] = table_seq_no
        state['id'] = table_id
        write_state(state_file, state)

#col_no start from 1
def numer_to_letter(col_no):
    col = col_no
    col_str = ""
    while col > 0:
        a = int((col - 1) / 26)
        b = (col -1) % 26
        col_str = chr(b + 65) + col_str
        col = a
    return col_str 

if __name__ == '__main__':
    main()
