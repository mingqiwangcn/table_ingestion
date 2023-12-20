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
        for line in tqdm(f):
            table_data = json.loads(line)
            if len(table_data['rows']) == 0:
                continue
            yield table_data

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
    num_tables = 0
    num_block = 0
    out_data = []
    out_sql_info_file = os.path.join(out_dir, 'sql_info.jsonl')
    f_o_sql = open(out_sql_info_file, 'w')
    for table_sql_lst in generator.generate_questions(table_iterator):
        if len(table_sql_lst) == 0:
            continue
        table_id = table_sql_lst[0]['meta']['table_id']
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
        
        num_tables += 1
        if num_tables >= 100:
            num_block += 1
            out_file = os.path.join(out_dir, f'question_part_{num_block}.csv')
            df = pd.DataFrame(data=out_data, columns=out_col_names)
            df.to_csv(out_file)
            out_data = 0
            num_tables = 0
   
    if len(out_data) > 0:
        num_block += 1
        out_file = os.path.join(out_dir, f'question_part_{num_block}.csv')
        df = pd.DataFrame(data=out_data, columns=out_col_names)
        df.to_csv(out_file)
    
    f_o_sql.close()

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
