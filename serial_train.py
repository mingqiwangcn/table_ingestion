import os
import argparse
from tqdm import tqdm
import torch
import random
import json
from serial_block import BlockSerializer

def load_passages(passage_file):
    data = []
    with open(passage_file) as f:
        for line in tqdm(f):
            data.append(line)
    return data

def sample_train_data(passage_file, table_dir):
    passage_dir = os.path.dirname(passage_file)
    train_file = os.path.join(passage_dir, 'train_passages.jsonl')
    dev_file = os.path.join(passage_dir, 'dev_passages.jsonl')
    if not os.path.isfile(train_file):
        passage_data = load_passages(passage_file)
        K = min(len(passage_data), 10000)
        sample_passages = random.sample(passage_data, K)
        passage_info_lst = uncompress_passages(sample_passages, table_dir)
        num_train = int(len(passage_info_lst) * 0.8)
        train_passage_lst = passage_info_lst[:num_train]
        dev_passage_lst = passage_info_lst[num_train:]
        save_train(train_passage_lst, train_file)
        save_train(dev_passage_lst, dev_file)
    else:
        train_passage_lst = read_train(train_file)
        dev_passage_lst = read_train(dev_file)
    return train_passage_lst, dev_passage_lst

def save_train(passage_info_lst, passage_file):
    with open(passage_file, 'w') as f_o:
        for passage_info in passage_info_lst:
            f_o.write(json.dumps(passage_info) + '\n')

def read_train(passage_file):
    passage_info_lst = []
    with open(passage_file) as f:
        for line in f:
            passage_info = json.loads(line)
            passage_info_lst.append(passage_info)
    return passage_info_lst

def uncompress_passages(compressed_passages, table_dir):
    tsl = BlockSerializer()
    passage_info_lst = []
    table_dict = {}
    for text in tqdm(compressed_passages, desc='uncompress'):
        passage_info = json.loads(text)
        set_full_serial(passage_info, table_dict, table_dir, tsl)
    return passage_info_lst

def load_table(table_id, table_dir):
    table_file = os.path.join(table_dir, table_id + '.jsonl')
    with open(table_file) as f:
        table_data = json.load(f)
    return table_data

def set_full_serial(passage_info, table_dict, table_dir, tsl):
    tag_info = passage_info['tag']
    table_id = tag_info['table_id']
    if table_id not in table_dict:
        table_data = load_table(table_id, table_dir)
        table_dict[table_id] = table_data
    table_data = table_dict[table_id]
    serial_row_col = get_serial_row_col(tag_info)
    table_data['serial_row_col'] = serial_row_col 
    serial_block_lst = []
    for serial_block in tsl.serialize(table_data):
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
    passage_file = os.path.join('./output', args.dataset, args.strategy, 'passages.jsonl')
    table_dir = os.path.join('../data', args.dataset, 'tables')
    print('sample_train_data 1')
    train_passage_lst, dev_passage_lst = sample_train_data(passage_file, table_dir)
    print('sample_train_data 2')
def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', type=str, required=True)
    parser.add_argument('--strategy', type=str, required=True)
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()
