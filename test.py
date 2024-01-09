import json
from tqdm import tqdm
import os
from serial_block import BlockSerializer
from serial_schema import SchemaSerializer 
from serial_compress import CompressSerializer
from serial_numeric import NumericSerializer
from serial_cpr_scm import CprScmSerializer 
from multiprocessing import Pool as ProcessPool
import argparse
import copy

def iterate_table(args):
    data_file = get_table_file(args) 
    with open(data_file) as f:
        for line in f:
            table_data = json.loads(line)
            yield table_data

def get_table_file(args):
    data_file = '../data/%s/tables/tables.jsonl' % args.dataset
    return data_file

def read_tables(args):
    data_file = get_table_file(args) 
    table_lst = []
    with open(data_file) as f:
        for line in tqdm(f):
            table_data = json.loads(line)
            table_lst.append(table_data)
    return table_lst

def init_worker(args):
    global tsl
    if args.strategy == 'block':
        tsl = BlockSerializer()
    elif args.strategy == 'schema':
        tsl = SchemaSerializer()
    elif args.strategy == 'compress':
        tsl = CompressSerializer()
    elif args.strategy == 'compress_numeric':
        tsl = CompressSerializer()
        tsl.set_numeric_serializer(NumericSerializer()) 
    elif args.strategy == 'cpr_scm':
        tsl = CprScmSerializer()
    else:
        raise ValueError('Strategy (%s) not supported.' % args.strategy)

def process_table(arg_info):
    table_data = arg_info['table_data']
    block_lst = []
    for serial_block in tsl.serialize(table_data):
        block_lst.append(serial_block)
    return block_lst

def get_out_file(args, part_name=None):
    folder_name = args.strategy if part_name is None else f'{args.strategy}_{part_name}'
    out_dir = './output/%s/%s' % (args.dataset, folder_name) 
    if not os.path.isdir(out_dir):
        os.makedirs(out_dir)
    out_file = os.path.join(out_dir, 'passages.jsonl')
    return out_file 

def main():
    args = get_args()
    block_id = 0

    if args.strategy != 'cpr_scm':
        out_file = get_out_file(args)
        if os.path.exists(out_file):
            answer = input('(%s) already exists. Continue?(y/n)' % out_file)
            if answer != 'y':
                return
    else:
        out_file_cpr = get_out_file(args, part_name='compress')
        out_file_scm = get_out_file(args, part_name='schema')
        for out_file in [out_file_cpr, out_file_scm]:
            if os.path.exists(out_file):
                answer = input('(%s) already exists. Continue?(y/n)' % out_file)
                if answer != 'y':
                    return
    f_o = None
    f_o_cpr = None
    f_o_scm = None
    if args.strategy != 'cpr_scm':
        f_o = open(out_file, 'w')
    else:
        f_o_cpr = open(out_file_cpr, 'w')        
        f_o_scm = open(out_file_scm, 'w') 

    if not args.debug:
        print('Loading tables')
        table_lst = read_tables(args)
        arg_info_lst = []
        for table_data in table_lst:
            if len(table_data['rows']) == 0:
                continue
            arg_info = {
                'table_data':table_data,
            }
            arg_info_lst.append(arg_info)
        
        if len(arg_info_lst) == 0:
            print('No table data')
            return
        work_pool = ProcessPool(initializer=init_worker, initargs=(args, ))
        for table_block_lst in tqdm(work_pool.imap_unordered(process_table, arg_info_lst), total=len(table_lst)):
            for serial_block in table_block_lst:
                serial_block['p_id'] = block_id
                block_id += 1
                do_write(args, f_o, f_o_cpr, f_o_scm, serial_block)
    else:
        init_worker(args)
        for table_data in tqdm(iterate_table(args)):
            arg_info = {'table_data':table_data}
            table_block_lst = process_table(arg_info)
            for serial_block in table_block_lst:
                serial_block['p_id'] = block_id
                block_id += 1
                do_write(args, f_o, f_o_cpr, f_o_scm, serial_block)
    
    if f_o is not None: 
        f_o.close()

    if f_o_cpr is not None:
        f_o_cpr.close()

    if f_o_scm is not None:
        f_o_scm.close()

def do_write(args, f_o, f_o_cpr, f_o_scm, serial_block):
    if args.strategy != 'cpr_scm' :
        f_o.write(json.dumps(serial_block) + '\n')
    else:
        cpr_passage_info = serial_block['passage']
        scm_passage_info = serial_block['scm_passage']
        
        cpr_special_tokens = serial_block['tag']['special_tokens']
        scm_special_tokens = []

        del serial_block['passage']
        del serial_block['scm_passage']
        
        write_cpr_scm(f_o_cpr, cpr_passage_info, cpr_special_tokens, serial_block) 
        write_cpr_scm(f_o_scm, scm_passage_info, scm_special_tokens, serial_block) 

def write_cpr_scm(f_o_block, passage_info, special_tokens, serial_block):
    out_block = {'passage':passage_info}
    for key in serial_block:
        out_block[key] = serial_block[key]
    out_block['tag']['special_tokens'] = special_tokens
    f_o_block.write(json.dumps(out_block) + '\n')

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', type=str, required=True)
    parser.add_argument('--strategy', type=str, required=True)
    parser.add_argument('--debug', type=int)
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()
