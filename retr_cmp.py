import json
import os
from tqdm import tqdm
import pandas as pd

K = 10
def read_retr(retr_file):
    retr_data = [] 
    with open(retr_file) as f:
        for line in tqdm(f):
            item = json.loads(line)
            q_id = item['id']
            q_text = item['question']
            table_id_lst = item['table_id_lst']
            ctx_lst = item['ctxs'][:K]
            out_passage_lst = []
            top_correct = False
            for ctx_info in ctx_lst:
                tag_info = ctx_info['tag']
                table_id = tag_info['table_id']
                correct = (table_id in table_id_lst)
                if correct:
                    top_correct = True
                out_passage = {
                    'passage':ctx_info['text'],
                    'correct':correct,
                    'table_id':table_id
                }
                out_passage_lst.append(out_passage)
            out_item = {
                    'q_id':q_id,
                    'question':q_text,
                    'table_id_lst':table_id_lst,
                    'top_correct':top_correct,
                    'passages':out_passage_lst
            }
            retr_data.append(out_item)

    return retr_data

def compare(retr_data_1, retr_data_2):
    assert len(retr_data_1) == len(retr_data_2)
    out_q_1y_2n = []
    out_q_1n_2y = []
    out_q_1y_2y = []
    out_q_1n_2n = []

    for offset, retr_item_1 in enumerate(retr_data_1):
        retr_item_2 = retr_data_2[offset]
        q_id = retr_item_1['q_id'] 
        question = retr_item_1['question']
        table_id_lst = ' , '.join(retr_item_1['table_id_lst'])
        assert q_id == retr_item_2['q_id']

        out_item = [q_id, question, table_id_lst]
        if retr_item_1['top_correct'] and (not retr_item_2['top_correct']):
            out_q_1y_2n.append(out_item)
        elif retr_item_2['top_correct'] and (not retr_item_1['top_correct']):
            out_q_1n_2y.append(out_item)
        elif retr_item_2['top_correct'] and retr_item_1['top_correct']:
            out_q_1y_2y.append(out_item)
        else: 
            out_q_1n_2n.append(out_item)
    
    return out_q_1y_2n, out_q_1n_2y, out_q_1y_2y, out_q_1n_2n

def output_retr_data(retr_data, out_file):
    out_data = []
    for retr_item in retr_data:
        passage_lst = retr_item['passages']
        for passage_info in passage_lst:
            out_item = [
                    retr_item['q_id'],
                    retr_item['question'],
                    ' '.join(retr_item['table_id_lst']),
                    retr_item['top_correct'],
                    passage_info['passage'],
                    passage_info['table_id'],
                    passage_info['correct']
            ]
            out_data.append(out_item)
    
    col_names = ['qid', 'question', 'table_id_lst', 'top correct', 'passage', 'table_id', 'correct']
    df = pd.DataFrame(out_data, columns=col_names) 
    df.to_csv(out_file)

def main():
    strategy_lst = ['block', 'schema', 'compress', 'compress_numeric']
    retr_dict = {} 
    for strategy in strategy_lst:
        retr_file = f'/home/cc/code/solo_work/data/nyc_open_1000/query/test/{strategy}/fusion_retrieved.jsonl'
        out_file = f'/home/cc/code/solo_work/data/nyc_open_1000/query/test/cmp_log/retr_top_correct_{strategy}.csv'
        with open(retr_file) as f:
            retr_data = read_retr(retr_file)
            retr_dict[strategy] = retr_data
            output_retr_data(retr_data, out_file)
    
    out_dir = '/home/cc/code/solo_work/data/nyc_open_1000/query/test/cmp_log'
    cmp_pairs = [('block', 'schema'), ('schema', 'compress'), ('compress', 'compress_numeric')]
    for pair in cmp_pairs:
        retr_data_1 = retr_dict[pair[0]]
        retr_data_2 = retr_dict[pair[1]]
        out_cmp_data = compare(retr_data_1, retr_data_2)
        out_name_lst = [None] * 4 
        out_name_lst[0] = f'cmp-{pair[0]}-y-{pair[1]}-n.csv'
        out_name_lst[1] = f'cmp-{pair[0]}-n-{pair[1]}-y.csv'
        out_name_lst[2] = f'cmp-{pair[0]}-y-{pair[1]}-y.csv'
        out_name_lst[3] = f'cmp-{pair[0]}-n-{pair[1]}-n.csv'
        for offset, out_name in enumerate(out_name_lst):
            out_file = os.path.join(out_dir, out_name_lst[offset])
            out_data = out_cmp_data[offset]
            df = pd.DataFrame(out_data, columns=['q_id', 'question', 'table_lst'])
            df.to_csv(out_file)

if __name__ == '__main__':
    main()
