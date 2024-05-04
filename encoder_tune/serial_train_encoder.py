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
import transformers
import torch.nn as nn
from serial_collator import SerialCollator
from collections import OrderedDict

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

def load_cpr_data(cpr_dir):
    special_token_dict = {}
    passge_file = os.path.join(cpr_dir, 'passages.jsonl')
    cpr_dict = {}
    with open(passge_file) as f:
        for line in f:
            item = json.loads(line)
            token_lst = item['tag'].get('special_tokens')
            if token_lst is not None:
                for token in token_lst:
                    special_token_dict[token] = True
            cpr_p_id = item['p_id']
            cpr_dict[cpr_p_id] = item
    special_token_lst = list(special_token_dict.keys())
    return cpr_dict, special_token_lst

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
    cpr_dict, special_token_lst = load_cpr_data(cpr_dir)
    num_train = int(len(cpr_emb_dict) * 0.8)
    cpr_p_id_lst = list(cpr_emb_dict.keys())
    random.shuffle(cpr_p_id_lst)
    train_cpr_p_id = cpr_p_id_lst[:num_train]
    dev_cpr_p_id = cpr_p_id_lst[num_train:]
    args.device = get_device(0)
    args.special_token_lst = special_token_lst
    train(args, train_cpr_p_id, dev_cpr_p_id, cpr_dict, cpr_emb_dict, base_emb_dict)

def get_tokenizer(special_token_lst):
    tokenizer = transformers.BertTokenizerFast.from_pretrained('bert-base-uncased')
    if len(special_token_lst) > 0:
        tokenizer.add_tokens(special_token_lst, special_tokens=True)
    return tokenizer    

def load_model(args):
    model = SerialEncoder(args.tokenizer_size)
    args.model = '../models/student_tqa_retriever_step_29500/pytorch_model.bin'
    if args.model is not None:
        state_dict = torch.load(args.model, map_location=args.device)
        model_state_dict = copy_model_weights(state_dict)
        model.load_state_dict(model_state_dict)
    model = model.to(args.device)
    if len(args.special_token_lst) > 0:
        model.model.resize_token_embeddings(args.tokenizer_size)
    return model

def copy_model_weights(retr_state_dict):
    model_state_dict = OrderedDict()
    tag = 'ctx_encoder.'
    for key in retr_state_dict:
        if key.startswith(tag):
            update_key = key[len(tag):]
            model_state_dict[update_key] = retr_state_dict[key]
    return model_state_dict
    
def train(args, train_cpr_p_id, dev_cpr_p_id, cpr_dict, cpr_emb_dict, base_emb_dict):
    tokenizer = get_tokenizer(args.special_token_lst)
    args.tokenizer_size = len(tokenizer)
    model = load_model(args)
    collator = SerialCollator(tokenizer)

    learing_rate = 1e-3
    optimizer = optim.Adam(model.parameters(), lr=learing_rate)
    loss_func = nn.MSELoss()
    metric_dict = {}
    max_epoch = 200
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
            batch_cpr_passage = [cpr_dict[a] for a in batch_cpr_p_id]
            cpr_text_ids = collator(batch_cpr_passage).to(args.device)
            batch_cpr_emb = get_batch_emb(batch_cpr_p_id, cpr_emb_dict).to(args.device)
            updated_cpr_emb = model(cpr_text_ids, batch_cpr_emb)
            batch_loss = calc_loss(args, loss_func, updated_cpr_emb, 
                                   train_cpr_p_id, cpr_dict, base_emb_dict)
            #print(f'train epoch={epoch} loss={batch_loss.item()} {num_batch}/{total_batch}')
            optimizer.zero_grad()
            batch_loss.backward()
            optimizer.step()
        
        save_model(args, epoch, model)
        evaluate(args, epoch, model, 
                 collator, dev_cpr_p_id, cpr_dict, 
                 cpr_emb_dict, base_emb_dict, metric_dict)
        
def save_model(args, epoch, model):
    file_name = f'epoch_{epoch}_model.pt'
    out_path = os.path.join(args.train_out_dir, file_name)
    torch.save(model.state_dict(), out_path) 
    return out_path

def evaluate(args, epoch, model, 
             collator, dev_cpr_p_id, cpr_dict,
             cpr_emb_dict, base_emb_dict, metric_dict):
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
            batch_cpr_passage = [cpr_dict[a] for a in batch_cpr_p_id]
            cpr_text_ids = collator(batch_cpr_passage).to(args.device)
            batch_cpr_emb = get_batch_emb(batch_cpr_p_id, cpr_emb_dict).to(args.device)
            updated_cpr_emb = model(cpr_text_ids, batch_cpr_emb)
            batch_loss = calc_loss(args, loss_func, updated_cpr_emb, 
                                   dev_cpr_p_id, cpr_dict, base_emb_dict)
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

def calc_loss(args, loss_func, updated_cpr_emb, train_cpr_p_id, cpr_dict, base_emb_dict):
    total_loss = 0
    for offset, cpr_emb in enumerate(updated_cpr_emb):
        cpr_p_id = train_cpr_p_id[offset]
        base_p_id_lst = cpr_dict[cpr_p_id]['base_p_id_lst']
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
    parser.add_argument('--batch_size', type=int, default=32)
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()
