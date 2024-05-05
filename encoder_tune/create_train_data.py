import json
import os
import argparse
import glob
import random
from tqdm import tqdm

def read_tables(args):
    file_pattern = os.path.join(args.sample_dir, 'tables/*.jsonl')
    table_file_lst = glob.glob(file_pattern)
    table_dict = {}
    table_number_map = {}
    for table_file in tqdm(table_file_lst):
        with open(table_file) as f:
            for line in f:
                table_data = json.loads(line)
                table_id = table_data['tableId']
                table_number = table_data['table_number']
                table_number_map[table_number] = table_id
                if table_id not in table_dict:
                    table_dict[table_id] = []
                table_sub_lst = table_dict[table_id]
                table_sub_lst.append(table_data)
    return table_dict, table_number_map

def read_questions(args, cpr_tab_no_map):
    question_file = os.path.join(args.sample_dir, 'questions.jsonl')
    q_info_lst = []
    with open(question_file) as f:
        for line in tqdm(f):
            q_info = json.loads(line)
            q_info_lst.append(q_info)
    return split_questions(q_info_lst, cpr_tab_no_map)

def read_cpr_passages(args):
    p_tab_no_map = {}
    p_tab_id_map = {}
    cpr_passage_file = os.path.join(args.sample_dir, 'sample_cpr_passages.jsonl')
    with open(cpr_passage_file) as f:
        for line in tqdm(f):
            p_info = json.loads(line)
            table_number = p_info['table_number']
            table_id = p_info['tag']['table_id']
            p_tab_no_map[table_number] = p_info
            if table_id not in p_tab_id_map:
                p_tab_id_map[table_id] = []
            sub_cpr_lst = p_tab_id_map[table_id]
            sub_cpr_lst.append(p_info)
    return p_tab_no_map, p_tab_id_map

def read_base_passages(args):
    p_dict = {}
    base_passage_file = os.path.join(args.sample_dir, 'sample_base_passages.jsonl')
    with open(base_passage_file) as f:
        for line in tqdm(f):
            p_info = json.loads(line)
            p_id = p_info['p_id']
            p_dict[p_id] = p_info
    return p_dict

def main():
    args = get_args()
    work_dir = os.path.dirname(os.getcwd())
    args.work_dir = work_dir
    args.sample_dir = os.path.join(work_dir, 'output', args.dataset, args.strategy, 'samples')
    train_file = os.path.join(args.sample_dir, 'train.jsonl')
    if os.path.isfile(train_file):
        print(f'{train_file} already exists')
        return
    dev_file = os.path.join(args.sample_dir, 'dev.jsonl')
    if os.path.isfile(dev_file):
        print(f'{dev_file} already exists')
        return
    cpr_tab_no_map, cpr_tab_id_map = read_cpr_passages(args)
    table_dict, table_number_map = read_tables(args)
    train_q_info_lst, dev_q_info_lst = read_questions(args, cpr_tab_no_map)
    base_map = read_base_passages(args)
    with open(train_file, 'w') as f_o_train_cpr:
        create_data(train_q_info_lst, cpr_tab_no_map, cpr_tab_id_map, base_map, f_o_train_cpr)
    with open(dev_file, 'w') as f_o_dev_cpr:
        create_data(dev_q_info_lst, cpr_tab_no_map, cpr_tab_id_map, base_map, f_o_dev_cpr)

def split_questions(q_info_lst, cpr_tab_no_map):
    q_dict = {}
    for q_info in q_info_lst:
        table_no = q_info['table_number']
        table_id = cpr_tab_no_map[table_no]['tag']['table_id']
        if table_id not in q_dict:
            q_dict[table_id] = []
        group_q_lst = q_dict[table_id]
        group_q_lst.append(q_info)
    table_id_lst = list(q_dict.keys())
    random.shuffle(table_id_lst)
    num_train = int(len(table_id_lst) * 0.8)
    train_table_id_lst = table_id_lst[:num_train]
    dev_table_id_lst = table_id_lst[num_train:]
    train_q_lst = []
    for table_id in train_table_id_lst:
        train_q_lst.extend(q_dict[table_id])
    dev_q_lst = []
    for table_id in dev_table_id_lst:
        dev_q_lst.extend(q_dict[table_id])
    return train_q_lst, dev_q_lst

def create_data(q_info_lst, cpr_tab_no_map, cpr_tab_id_map, base_map, f_o_cpr):
    for q_id, q_info in tqdm(enumerate(q_info_lst), total=len(q_info_lst)):
        q_table_number = q_info['table_number']
        q_table_id = cpr_tab_no_map[q_table_number]['tag']['table_id']
        cpr_example = {
            'table_number':q_table_number,
            'table_id':q_table_id,
            'id':q_id,
            'q_id':q_id,
            'question':q_info['question'],
            'answers':[],
            'target':'',
            'ctx':[],
            'pos_idxes':[],
            'neg_idxes':[]
        }
        pos_table_id = q_table_id
        cpr_pos_passge_info = cpr_tab_no_map[q_table_number]
        cpr_base_p_id_lst = cpr_pos_passge_info['base_p_id_lst']
        cpr_pos_ctx = {
            'title':'',
            'text':cpr_pos_passge_info['passage'],
            'base_text_lst':get_base_text_samples(cpr_base_p_id_lst, base_map, 3),
            'score':1.0
        }
        cpr_neg_passage_samples = get_cpr_neg_passage_samples(pos_table_id, cpr_tab_id_map, 20)
        cpr_neg_ctx_lst = [{
                              'title':'',
                              'text':a,
                              'base_text_lst':get_base_text_samples(a['base_p_id_lst'], base_map, 3),
                              'score':1.0 
                           } for a in cpr_neg_passage_samples]
        cpr_example['ctx'] = [cpr_pos_ctx] + cpr_neg_ctx_lst
        cpr_example['pos_idxes'] = [0]
        cpr_example['neg_idxes'] = list(range(1, len(cpr_example['ctx'])))
        f_o_cpr.write(json.dumps(cpr_example) + '\n')

def get_base_text_samples(base_p_id_lst, base_map, num_samples):
    M = min(len(base_p_id_lst), num_samples)
    sample_base_p_id_lst = random.sample(base_p_id_lst, M)
    sample_text_lst = [base_map[a]['passage'] for a in sample_base_p_id_lst]
    return sample_text_lst

def get_cpr_neg_passage_samples(pos_table_id, p_tab_id_map, num_neg_samples):
    neg_passage_lst = []
    for table_id in p_tab_id_map:
        if table_id == pos_table_id:
            continue
        neg_passage_lst.extend(p_tab_id_map[table_id])
    M = min(num_neg_samples, len(neg_passage_lst))
    neg_passage_samples = random.sample(neg_passage_lst, M)
    return neg_passage_samples

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', type=str, required=True)
    parser.add_argument('--strategy', type=str, required=True)
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()