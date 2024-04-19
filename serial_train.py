import os
import argparse
from tqdm import tqdm
import torch
import random
import json
from multiprocessing import Pool as ProcessPool
from serial_block import BlockSerializer
import uuid

def init_worker(args):
    global g_tsl
    global g_table_dir
    global g_passage_dir
    g_tsl = BlockSerializer()
    g_table_dir = os.path.join('../data', args.dataset, 'tables')
    g_passage_dir = os.path.join('./output', args.dataset, args.strategy, 'sample_parts')

def load_passages(passage_file):
    data = []
    with open(passage_file) as f:
        for line in tqdm(f):
            data.append(line)
    return data

def sample_train_data(passage_file, args):
    passage_dir = os.path.dirname(passage_file)
    sample_file = os.path.join(passage_dir, 'sample_passages.jsonl')
    if not os.path.isfile(sample_file):
        passage_data = load_passages(passage_file)
        K = min(len(passage_data), 10000)
        sample_passages = random.sample(passage_data, K)
        uncompress_passages(sample_passages, sample_file, args)

    passage_info_lst = load_sample_passges(sample_file)
    num_train = int(len(passage_info_lst) * 0.8)
    train_passage_lst = passage_info_lst[:num_train]
    dev_passage_lst = passage_info_lst[num_train:]
    return train_passage_lst, dev_passage_lst

def load_sample_passges(passage_file):
    passage_info_lst = []
    with open(passage_file) as f:
        for line in tqdm(f):
            passage_info = json.loads(line)
            passage_info_lst.append(passage_info)
    return passage_info_lst
        
def uncompress_passages(compressed_passages, sample_file, args):
    task_info_lst = [{'text':a, 'task_no':offset} for offset, a in enumerate(compressed_passages)]
    num_workers = os.cpu_count()
    work_pool = ProcessPool(num_workers, initializer=init_worker, initargs=(args, ))
    task_batch_size = 1000
    num_tasks = len(task_info_lst)
    for start_pos in range(0, num_tasks, task_batch_size):
        end_pos = min(start_pos + task_batch_size, num_tasks)
        batch_task_lst = task_info_lst[start_pos:end_pos]
        batch_task_num = len(batch_task_lst)
        for _ in tqdm(work_pool.imap_unordered(process_passage, batch_task_lst), total=batch_task_num):
            continue
            
def process_passage(task_info):
    text = task_info['text']
    task_no = task_info['task_no']
    passage_info = json.loads(text)
    set_full_serial(passage_info)
    out_file = os.path.join(g_passage_dir, f'sample_passages_part_{task_no}.jsonl')
    with open(out_file, 'w') as f_o:
        f_o.write(json.dumps(passage_info))

def load_table(table_id, table_dir):
    table_file = os.path.join(table_dir, table_id + '.jsonl')
    with open(table_file) as f:
        table_data = json.load(f)
    return table_data

def set_full_serial(passage_info):
    tag_info = passage_info['tag']
    table_id = tag_info['table_id']
    table_data = load_table(table_id, g_table_dir)
    serial_row_col = get_serial_row_col(tag_info)
    table_data['serial_row_col'] = serial_row_col 
    serial_block_lst = []
    for serial_block in g_tsl.serialize(table_data):
        serial_block_lst.append(serial_block)
    passage_info['full_serial'] = serial_block_lst
    del table_data['serial_row_col']
    
def get_serial_row_col(tag_info):
    row_lst = tag_info['row']
    col_set_lst = tag_info['cols']
    serial_row_col = []
    for offset, row in enumerate(row_lst):
        col_group_lst = col_set_lst[offset]
        col_lst = []
        for col_group in col_group_lst:
            col_lst.extend(col_group)
        col_lst.sort()
        item = [row, col_lst]
        serial_row_col.append(item)
    return serial_row_col

def main():
    args = get_args()
    sample_dir = os.path.join('./output', args.dataset, args.strategy, 'sample_parts')
    if not os.path.isdir(sample_dir):
        os.makedirs(sample_dir)

    passage_file = os.path.join('./output', args.dataset, args.strategy, 'passages.jsonl')
    
    print('sample_train_data 1')
    train_passage_lst, dev_passage_lst = sample_train_data(passage_file, args)
    print('sample_train_data 2')
def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', type=str, required=True)
    parser.add_argument('--strategy', type=str, required=True)
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()
