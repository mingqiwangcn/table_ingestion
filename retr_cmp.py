import json
import os
from tqdm import tqdm
import pandas as pd

CMP_Dataset = 'nyc_open_100K'
CMP_Top_Num = 10
 
def read_retr(retr_file):
    retr_data = []
    with open(retr_file) as f:
        for line in tqdm(f):
            item = json.loads(line)
            passage_lst = item['passages']
            retr_data.append(item)
    return retr_data

def compare_retr(retr_data_1, retr_data_2):
    assert len(retr_data_1) == len(retr_data_2)
    out_q_1y_2n = []
    out_q_1n_2y = []
    out_q_1y_2y = []
    out_q_1n_2n = []

    for offset, retr_item_1 in enumerate(retr_data_1):
        retr_item_2 = retr_data_2[offset]
        q_id = retr_item_1['id'] 
        question = retr_item_1['question']
        table_id_lst = ' , '.join(retr_item_1['table_id_lst'])
        assert q_id == retr_item_2['id']

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
                    retr_item['id'],
                    retr_item['question'],
                    ' '.join(retr_item['table_id_lst']),
                    retr_item['top_correct'],
                    passage_info['passage'],
                    passage_info['table_id'],
                    passage_info['correct'],
                    passage_info['row'],
            ]
            out_data.append(out_item)
    
    col_names = ['qid', 'question', 'table_id_lst', 'top correct', 'passage', 'table_id', 'correct', 'row']
    df = pd.DataFrame(out_data, columns=col_names) 
    df.to_csv(out_file)

def main():
    strategy_lst = ['block', 'schema']
    retr_dict = {} 
    for strategy in strategy_lst:
        retr_dir = f'/home/cc/code/solo_work/data/{CMP_Dataset}/query/test/{strategy}/exact'
        retr_file = os.path.join(retr_dir, f'retr_{strategy}_top_{CMP_Top_Num}.jsonl')
        out_file = os.path.join(retr_dir, f'retr_{strategy}_top_{CMP_Top_Num}.csv')
        with open(retr_file) as f:
            retr_data = read_retr(retr_file)
            retr_dict[strategy] = retr_data
            output_retr_data(retr_data, out_file)
        print(f'output {out_file}\n')
    
    out_dir = f'/home/cc/code/solo_work/data/{CMP_Dataset}/query/test/cmp_log_top_{CMP_Top_Num}'
    if not os.path.isdir(out_dir):
        os.makedirs(out_dir)
    cmp_pairs = [('block', 'schema')]
    for pair in cmp_pairs:
        retr_data_1 = retr_dict[pair[0]]
        retr_data_2 = retr_dict[pair[1]]
        import pdb; pdb.set_trace()
        out_cmp_data = compare_retr(retr_data_1, retr_data_2)
        out_name_lst = [None] * 4 
        out_name_lst[0] = f'cmp-top-{CMP_Top_Num}-{pair[0]}-y-{pair[1]}-n-{len(out_cmp_data[0])}.csv'
        out_name_lst[1] = f'cmp-top-{CMP_Top_Num}-{pair[0]}-n-{pair[1]}-y-{len(out_cmp_data[1])}.csv'
        out_name_lst[2] = f'cmp-top-{CMP_Top_Num}-{pair[0]}-y-{pair[1]}-y-{len(out_cmp_data[2])}.csv'
        out_name_lst[3] = f'cmp-top-{CMP_Top_Num}-{pair[0]}-n-{pair[1]}-n-{len(out_cmp_data[3])}.csv'
        for offset, out_name in enumerate(out_name_lst):
            out_file = os.path.join(out_dir, out_name_lst[offset])
            out_data = out_cmp_data[offset]
            df = pd.DataFrame(out_data, columns=['q_id', 'question', 'table_lst'])
            df.to_csv(out_file)
            print(f'output {out_file}')

if __name__ == '__main__':
    main()
