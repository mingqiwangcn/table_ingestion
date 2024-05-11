import os
import argparse
from tqdm import tqdm
import torch
import random
import json
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

def get_device(cuda):
    device = torch.device(("cuda:%d" % cuda) if torch.cuda.is_available() and cuda >=0 else "cpu")
    return device

def main():
    args = get_args()
    args.out_dir = os.path.join('./output', args.dataset, args.strategy, 'updated_emb')
    if not os.path.isdir(args.out_dir):
        os.makedirs(args.out_dir)
    else:
        file_pattern = os.path.join(args.out_dir, '*_embeddings_part_*')
        if len(glob.glob(file_pattern)) > 0:
            print(f'Embeddings already exist in {args.out_dir} ')
            return
    args.device = get_device(0)
    passage_info_lst, special_token_lst = load_cpr_data(args)
    args.special_token_lst = special_token_lst
    tokenizer = get_tokenizer(args.special_token_lst)
    model = load_model(args)
    collator = SerialCollator(tokenizer)
    encode_passages(args, model, collator, passage_info_lst)

def load_cpr_data(args):
    special_token_dict = {}
    cpr_dir = os.path.join('./output', args.dataset, args.strategy)
    passge_file = os.path.join(cpr_dir, 'passages.jsonl')
    passage_info_lst = []
    with open(passge_file) as f:
        for line in tqdm(f):
            item = json.loads(line)
            passage_info_lst.append(item)
            token_lst = item['tag'].get('special_tokens')
            if token_lst is not None:
                for token in token_lst:
                    special_token_dict[token] = True
            
    special_token_lst = list(special_token_dict.keys())
    return passage_info_lst, special_token_lst

def get_tokenizer(special_token_lst):
    tokenizer = transformers.BertTokenizerFast.from_pretrained('bert-base-uncased')
    if len(special_token_lst) > 0:
        tokenizer.add_tokens(special_token_lst, special_tokens=True)
    return tokenizer    

def load_model(args):
    model = SerialEncoder(None)
    args.model = './output/nyc_open_1000/schema/train_2024_4_21_23_38_41/epoch_5_model.pt'
    if args.model is not None:
        state_dict = torch.load(args.model, map_location=args.device)
        model.load_state_dict(state_dict)
    model = model.to(args.device)
    if len(args.special_token_lst) > 0:
        model.model.resize_token_embeddings(args.tokenizer_size)
    return model
    
def encode_passages(args, model, collator, passage_info_lst):
    num_batch = 0
    total_batch = len(passage_info_lst) // args.batch_size
    if len(passage_info_lst) % args.batch_size:
        total_batch += 1
    enc_bar = tqdm(total=len(passage_info_lst))
    with torch.no_grad():
        model.eval()
        file_part = 0
        for offset in range(0, len(passage_info_lst), args.batch_size):
            num_batch += 1
            pos = offset + args.batch_size
            batch_cpr_passage = passage_info_lst[offset:pos]
            batch_p_id = [a['p_id'] for a in batch_cpr_passage]
            cpr_text_ids = collator(batch_cpr_passage).to(args.device)
            out_cpr_emb = model(cpr_text_ids, None).cpu().numpy()
            out_data = [batch_p_id, out_cpr_emb]
            out_file = os.path.join(args.out_dir, 'passages.jsonl_embeddings_part_' + str(file_part))
            file_part += 1
            with open(out_file, mode='wb') as f_o:
                pickle.dump(out_data, f_o)         
            enc_bar.update(len(batch_p_id))

def get_device(cuda):
    device = torch.device(("cuda:%d" % cuda) if torch.cuda.is_available() and cuda >=0 else "cpu")
    return device

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', type=str, required=True)
    parser.add_argument('--strategy', type=str, required=True)
    parser.add_argument('--model', type=str)
    parser.add_argument('--batch_size', type=int, default=500)
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()
