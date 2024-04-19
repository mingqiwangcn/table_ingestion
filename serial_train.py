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
from serial_encoder import SerialEncoder
import datetime
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
        uncompress_passages(sample_passages, sample_file, args)

    out_cpr_file = os.path.join(passage_dir, 'sample_cpr_passages.jsonl')
    out_base_file = os.path.join(passage_dir, 'sample_base_passages.jsonl')
    f_o_cpr = open(out_cpr_file, 'w')
    f_o_base = open(out_base_file, 'w')
    merge_passges(sample_file_pattern, f_o_cpr, f_o_base)
    f_o_cpr.close()
    f_o_base.close()
    
    '''
    num_train = int(len(passage_info_lst) * 0.8)
    train_passage_lst = passage_info_lst[:num_train]
    dev_passage_lst = passage_info_lst[num_train:]
    return train_passage_lst, dev_passage_lst
    '''

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

def create_samples():
    args = get_args()
    sample_dir = os.path.join('./output', args.dataset, args.strategy, 'sample_parts')
    if not os.path.isdir(sample_dir):
        os.makedirs(sample_dir)
    passage_file = os.path.join('./output', args.dataset, args.strategy, 'passages.jsonl')
    sample_train_data(passage_file, args)

def load_emb(emb_file):
    p_id_lst = []
    p_emb_lst = []
    with open(emb_file, 'rb') as f:
        while True:
            try:
                batch_p_id, batch_p_emb = pickle.load(f)
                p_id_lst.append(batch_p_id)
                p_emb_lst.append(batch_p_emb)
            except EOFError:
                break
    all_p_id = [p_id for batch in p_id_lst for p_id in batch]
    all_p_emb = np.concatenate(p_emb_lst, axis=0)
    return all_p_id, all_p_emb

def load_strategy_emb(emb_dir):
    file_pattern = os.path.join(emb_dir, 'passages.jsonl_embeddings_part_*')
    emb_file_lst = glob.glob(file_pattern)
    emb_dict = {}
    for emb_file in emb_file_lst:
        batch_p_id, batch_emb = load_emb(emb_file)
        for offset, p_id in enumerate(batch_p_id):
            p_emb = torch.tensor(batch_emb[offset]).to(torch.float32)
            emb_dict[p_id] = {'emb':p_emb}
    return emb_dict

def load_cpr_base_map(cpr_dir):
    passge_file = os.path.join(cpr_dir, 'passages.jsonl')
    cpr_base_map = {}
    with open(passge_file) as f:
        for line in f:
            item = json.loads(line)
            cpr_p_id = item['p_id']
            base_p_id_lst = item['base_p_id_lst']
            cpr_base_map[cpr_p_id] = base_p_id_lst
    return cpr_base_map

def get_date_str():
    stamp_info = datetime.datetime.now()
    date_str = f'{stamp_info.year}_{stamp_info.month}_{stamp_info.day}_{stamp_info.hour}_{stamp_info.minute}_{stamp_info.second}'
    return date_str

def main():
    args = get_args()
    args.train_out_dir = os.path.join('./output', args.dataset, args.strategy, 'train_' + get_date_str())
    if not os.path.isdir(args.train_out_dir):
        os.makedirs(args.train_out_dir)
    cpr_dir = os.path.join('./output', args.dataset, args.strategy + '_cpr')
    cpr_emb_dir = os.path.join(cpr_dir, 'emb')
    cpr_emb_dict = load_strategy_emb(cpr_emb_dir)
    base_dir = os.path.join('./output', args.dataset, args.strategy + '_base')
    base_emb_dir = os.path.join(base_dir, 'emb')
    base_emb_dict = load_strategy_emb(base_emb_dir)
    cpr_base_map = load_cpr_base_map(cpr_dir)
    num_train = int(len(cpr_emb_dict) * 0.8)
    cpr_p_id_lst = list(cpr_emb_dict.keys())
    random.shuffle(cpr_p_id_lst)
    train_cpr_p_id = cpr_p_id_lst[:num_train]
    dev_cpr_p_id = cpr_p_id_lst[num_train:]
    args.device = get_device(0)
    train(args, train_cpr_p_id, dev_cpr_p_id, cpr_emb_dict, base_emb_dict, cpr_base_map)

def load_model(args):
    model = SerialEncoder()
    if args.model is not None:
        state_dict = torch.load(args.model, map_location=args.device)
        model.load_state_dict(state_dict)
    model = model.to(args.device)
    return model
    
def train(args, train_cpr_p_id, dev_cpr_p_id, cpr_emb_dict, base_emb_dict, cpr_base_map):
    model = load_model(args)
    learing_rate = 1e-3
    optimizer = optim.Adam(model.parameters(), lr=learing_rate)
    loss_func = nn.MSELoss()
    metric_dict = {}
    max_epoch = 100
    for epoch in range(max_epoch):
        model.train()
        random.shuffle(train_cpr_p_id)
        num_batch = 0
        total_batch = len(train_cpr_p_id) // args.batch_size
        if len(train_cpr_p_id) % args.batch_size:
            total_batch += 1
        for offset in range(0, len(train_cpr_p_id), args.batch_size):
            num_batch += 1
            pos = offset + args.batch_size
            batch_cpr_p_id = train_cpr_p_id[offset:pos]
            batch_cpr_emb = get_batch_emb(batch_cpr_p_id, cpr_emb_dict).to(args.device)
            updated_cpr_emb = model(batch_cpr_emb)
            batch_loss = calc_loss(args, loss_func, updated_cpr_emb, train_cpr_p_id, cpr_base_map, base_emb_dict)
            print(f'train epoch={epoch} loss={batch_loss.item()} {num_batch}/{total_batch}')
            optimizer.zero_grad()
            batch_loss.backward()
            optimizer.step()
        
        save_model(args, epoch, model)
        evaluate(args, epoch, model, dev_cpr_p_id, cpr_emb_dict, base_emb_dict, cpr_base_map, metric_dict)
        
def save_model(args, epoch, model):
    file_name = 'model.pt'
    out_path = os.path.join(args.train_out_dir, file_name)
    torch.save(model.state_dict(), out_path) 
    return out_path

def evaluate(args, epoch, model, dev_cpr_p_id, cpr_emb_dict, 
             base_emb_dict, cpr_base_map, metric_dict):
    num_batch = 0
    total_batch = len(dev_cpr_p_id) // args.batch_size
    if len(dev_cpr_p_id) % args.batch_size:
        total_batch += 1
    with torch.no_grad():
        model.eval()
        total_loss = 0
        loss_func = nn.MSELoss()
        count = 0
        for offset in range(0, len(dev_cpr_p_id), args.batch_size):
            num_batch += 1
            pos = offset + args.batch_size
            batch_cpr_p_id = dev_cpr_p_id[offset:pos]
            count += len(batch_cpr_p_id)
            batch_cpr_emb = get_batch_emb(batch_cpr_p_id, cpr_emb_dict).to(args.device)
            updated_cpr_emb = model(batch_cpr_emb)
            batch_loss = calc_loss(args, loss_func, updated_cpr_emb, dev_cpr_p_id, cpr_base_map, base_emb_dict)
            total_loss += batch_loss.item() * len(updated_cpr_emb)
            
        loss_score = total_loss / len(dev_cpr_p_id)
        print(f'evaluate epoch={epoch} loss={loss_score}')
        metric_dict[epoch] = {'loss':loss_score}
        if 'best' not in metric_dict:
            metric_dict['best'] = {'epoch':epoch, 'loss':loss_score, 'patience':0}
        else:
            best_info = metric_dict['best']
            if loss_score < best_info['loss']:
                best_info['epoch'] = epoch
                best_info['loss'] = loss_score
                best_info['patience'] = 0
            else:
                best_info['patience'] += 1
        
        out_metric_file = os.path.join(args.train_out_dir, 'metric.json')
        with open(out_metric_file, 'w') as f_o:
            f_o.write(json.dumps(metric_dict))

def get_device(cuda):
    device = torch.device(("cuda:%d" % cuda) if torch.cuda.is_available() and cuda >=0 else "cpu")
    return device

def calc_loss(args, loss_func, updated_cpr_emb, train_cpr_p_id, cpr_base_map, base_emb_dict):
    total_loss = 0
    for offset, cpr_emb in enumerate(updated_cpr_emb):
        cpr_p_id = train_cpr_p_id[offset]
        base_p_id_lst = cpr_base_map[cpr_p_id]
        M = len(base_p_id_lst)
        base_emb = get_batch_emb(base_p_id_lst, base_emb_dict).view(M, -1).to(args.device)
        cpr_emb_expand = cpr_emb.view(1, -1).expand(M, -1)
        loss = loss_func(cpr_emb_expand, base_emb)
        total_loss += loss
    batch_loss = total_loss / len(updated_cpr_emb)
    return batch_loss
        
def get_batch_emb(batch_p_id, emb_dict):
    emb_lst = [emb_dict[a]['emb'].view(1, -1) for a in batch_p_id]
    batch_emb = torch.cat(emb_lst, dim=0)
    return batch_emb

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', type=str, required=True)
    parser.add_argument('--strategy', type=str, required=True)
    parser.add_argument('--model', type=str)
    parser.add_argument('--batch_size', type=int, default=100)
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()
