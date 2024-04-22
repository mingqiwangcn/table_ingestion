import os
import argparse
from tqdm import tqdm
import torch
import random
import json
from multiprocessing import Pool as ProcessPool
from serial_block import BlockSerializer
import uuid
import glob
import pickle
import numpy as np
import torch.optim as optim
import datetime
import transformers
import torch.nn as nn

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
    sample_file_pattern = os.path.join(passage_dir, 'sample_parts/sample_passages_part_*.jsonl')
    sample_passage_file_lst = glob.glob(sample_file_pattern)
    if len(sample_passage_file_lst) == 0:
        passage_data = load_passages(passage_file)
        K = min(len(passage_data), 10000)
        sample_passages = random.sample(passage_data, K)
        uncompress_passages(sample_passages, args)

    out_cpr_file = os.path.join(passage_dir, 'sample_cpr_passages.jsonl')
    out_base_file = os.path.join(passage_dir, 'sample_base_passages.jsonl')
    f_o_cpr = open(out_cpr_file, 'w')
    f_o_base = open(out_base_file, 'w')
    merge_passges(sample_file_pattern, f_o_cpr, f_o_base)
    f_o_cpr.close()
    f_o_base.close()
    
def merge_passges(sample_file_pattern, f_o_cpr, f_o_base):
    passage_file_lst = glob.glob(sample_file_pattern)
    g_p_id = 0
    for passage_file in tqdm(passage_file_lst):
        with open(passage_file) as f:
            passage_info = json.load(f)
            base_passage_lst = passage_info['full_serial']
            base_p_id_lst = []
            for base_passage in base_passage_lst:
                base_passage['p_id'] = g_p_id
                f_o_base.write(json.dumps(base_passage) + '\n')
                base_p_id_lst.append(base_passage['p_id'])
                g_p_id += 1
            del passage_info['full_serial']
            passage_info['base_p_id_lst'] = base_p_id_lst
            f_o_cpr.write(json.dumps(passage_info) + '\n')
        
def uncompress_passages(compressed_passages, args):
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
    sample_train_data(passage_file, args)

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', type=str, required=True)
    parser.add_argument('--strategy', type=str, required=True)
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()
