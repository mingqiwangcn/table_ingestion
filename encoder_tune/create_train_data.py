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
    question_file = os.path.join(args.strategy_dir, 'questions.jsonl')
    q_info_lst = []
    q_id = 0
    with open(question_file) as f:
        for line in tqdm(f):
            q_info = json.loads(line)
            q_id += 1
            q_info['id'] = q_id
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

    args.strategy_dir = os.path.join(work_dir, 'output', args.dataset, args.strategy)
    args.sample_dir = os.path.join(args.strategy_dir, 'samples')

    train_dir = os.path.join(args.sample_dir, 'train')
    if os.path.isdir(train_dir):
        print(f'{train_dir} already exists')
        return
    os.mkdir(train_dir)

    train_cpr_file = os.path.join(train_dir, 'train_cpr.jsonl')
    train_base_file = os.path.join(train_dir, 'train_base.jsonl')
    train_cpr_base_offset_map_file = os.path.join(train_dir, 'train_cpr_base_map.jsonl')
    dev_cpr_file = os.path.join(train_dir, 'dev_cpr.jsonl')
    dev_base_file = os.path.join(train_dir, 'dev_base.jsonl')
    dev_cpr_base_offset_map_file = os.path.join(train_dir, 'dev_cpr_base_map.jsonl')
    
    cpr_tab_no_map, cpr_tab_id_map = read_cpr_passages(args)
    table_dict, table_number_map = read_tables(args)
    train_q_info_lst, dev_q_info_lst = read_questions(args, cpr_tab_no_map)
    base_map = read_base_passages(args)

    create_data(train_q_info_lst, cpr_tab_no_map, cpr_tab_id_map, 
                base_map, train_cpr_file, train_base_file, train_cpr_base_offset_map_file)
    
    create_data(dev_q_info_lst, cpr_tab_no_map, cpr_tab_id_map, 
                base_map, dev_cpr_file, dev_base_file, dev_cpr_base_offset_map_file)

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

def create_example(table_no, table_id, q_id, question):
    example = {
        'table_number':table_no,
        'table_id':table_id,
        'qid':q_id,
        'question':question,
        'answers':[],
        'target':'',
        'ctxs':[],
        'pos_idxes':[],
        'hard_neg_idxes':[],
        'special_tokens':[]
    }
    return example

def create_context(passage_info, special_token_set=None):
    ctx = {
        'title':'',
        'text':passage_info['passage'],
        'score':1.0
    }
    if special_token_set is not None:
        special_token_set.update(set(passage_info['tag']['special_tokens']))
    return ctx

def update_cpr_base_ctx_pair(offset_map, cpr_example, base_example, 
                             cpr_ctx, base_ctx_lst):
    cpr_offset = len(cpr_example['ctxs'])
    base_offset = len(base_example['ctxs'])
    assert (cpr_offset not in offset_map)
    offset_map[str(cpr_offset)] = [(base_offset + pos) for pos, a in enumerate(base_ctx_lst)]
    cpr_example['ctxs'].append(cpr_ctx)
    base_example['ctxs'] += base_ctx_lst

def create_data(q_info_lst, cpr_tab_no_map, cpr_tab_id_map, base_map, 
                cpr_file, base_file, cpr_base_offset_map_file):
    f_o_cpr = open(cpr_file, 'w')
    f_o_base = open(base_file, 'w')
    f_offset_map = open(cpr_base_offset_map_file, 'w')
    for q_info in tqdm(q_info_lst):
        q_id = q_info['id']
        q_table_number = q_info['table_number']
        q_table_id = cpr_tab_no_map[q_table_number]['tag']['table_id']
        question = q_info['question']
        cpr_example = create_example(q_table_number, q_table_id, q_id, question)
        base_example = create_example(q_table_number, q_table_id, q_id, question)
        pos_table_id = q_table_id
        special_token_set = set()
        cpr_pos_passge_info = cpr_tab_no_map[q_table_number]
        cpr_base_p_id_lst = cpr_pos_passge_info['base_p_id_lst']
        cpr_base_offset_map = {}
        cpr_pos_ctx = create_context(cpr_pos_passge_info, special_token_set)
        base_pos_ctx_lst = create_base_context_lst(cpr_base_p_id_lst, base_map, 3)
        
        update_cpr_base_ctx_pair(cpr_base_offset_map, cpr_example, base_example, 
                                 cpr_pos_ctx, base_pos_ctx_lst)
        
        cpr_neg_passage_samples = get_cpr_neg_passage_samples(pos_table_id, cpr_tab_id_map, 20)
        for cpr_neg_passage_info in cpr_neg_passage_samples:
            cpr_neg_ctx = create_context(cpr_neg_passage_info, special_token_set)
            base_neg_ctx_lst = create_base_context_lst(
                cpr_neg_passage_info['base_p_id_lst'], base_map, 3)
            update_cpr_base_ctx_pair(cpr_base_offset_map, cpr_example, base_example, 
                                 cpr_neg_ctx, base_neg_ctx_lst)

        cpr_example['special_tokens'] = list(special_token_set)
        cpr_example['pos_idxes'] = [0]
        cpr_example['hard_neg_idxes'] = list(range(1, len(cpr_example['ctxs'])))
        f_o_cpr.write(json.dumps(cpr_example) + '\n')
        f_o_base.write(json.dumps(base_example) + '\n')
        exa_offset_map = {'qid':q_id, 'offset_map':cpr_base_offset_map}
        f_offset_map.write(json.dumps(exa_offset_map) + '\n')
    f_o_cpr.close()
    f_o_base.close()
    f_offset_map.close()

def create_base_context_lst(base_p_id_lst, base_map, num_samples):
    M = min(len(base_p_id_lst), num_samples)
    sample_base_p_id_lst = random.sample(base_p_id_lst, M)
    ctx_lst = [create_context(base_map[a]) for a in sample_base_p_id_lst]
    return ctx_lst

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