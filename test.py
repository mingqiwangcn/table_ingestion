import json
from tqdm import tqdm
import os
from serial_block import BlockSerializer
from serial_schema import SchemaSerializer 
from serial_compress import CompressSerializer
from serial_numeric import NumericSerializer
from serial_cpr_scm import CprScmSerializer 
from serial_agree_coding import AgreeCodingSerializer
import multiprocessing
from multiprocessing import Pool as ProcessPool
import argparse
import copy
import glob

def iterate_table(args):
    data_file = get_table_file(args) 
    with open(data_file) as f:
        for line in f:
            table_data = json.loads(line)
            yield table_data

def get_table_files(args):
    file_pattern = '../data/%s/tables/*.jsonl' % args.dataset
    file_lst = glob.glob(file_pattern)
    return file_lst

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
    elif args.strategy == 'agree_coding':
        tsl = AgreeCodingSerializer()
    else:
        raise ValueError('Strategy (%s) not supported.' % args.strategy)

def process_table(arg_info):
    
    process_info = str(multiprocessing.current_process())

    task_id = arg_info['task_id']
    args = arg_info['args']
    table_file = arg_info['table_file']
    with open(table_file) as f:
        table_data = json.load(f)
    
    row_cnt = len(table_data['rows'])

    table_id = table_data['tableId']
    f_o_part = None
    f_o_cpr_part = None
    f_o_scm_part = None
    
    serial_file_name = f'{table_id}_serial.jsonl' 
    out_part_file = None
    out_cpr_part_file = None
    out_scm_part_file = None
    if args.strategy != 'cpr_scm':
        out_dir = arg_info['out_dir'] 
        out_part_file = os.path.join(out_dir, serial_file_name) 
        #f_o_part = open(out_part_file, 'w')
    else:
        out_dir_cpr = arg_info['out_dir_cpr']
        out_cpr_part_file = os.path.join(out_dir_cpr, serial_file_name)
        #f_o_cpr_part = open(out_cpr_part_file, 'w')

        out_dir_scm = arg_info['out_dir_scm']
        out_scm_part_file = os.path.join(out_dir_scm, serial_file_name)
        #f_o_scm_part = open(out_scm_part_file, 'w')

    serial_block_lst = []
    for serial_block in tsl.serialize(table_data):
        serial_block_lst.append(serial_block)
   
    return serial_block_lst
    #for serial_block in serial_block_lst:
    #    do_write(args, f_o_part, f_o_cpr_part, f_o_scm_part, serial_block)
    
    if f_o_part is not None:
        f_o_part.close()
    if f_o_cpr_part is not None:
        f_o_cpr_part.close()
    if f_o_scm_part is not None:
        f_o_scm_part.close()
    
    out_part_info = {
        'out_part_file':out_part_file,
        'out_cpr_part_file':out_cpr_part_file,
        'out_scm_part_file':out_scm_part_file
    }
    return serial_block_lst

def get_out_file(args, part_name=None):
    folder_name = args.strategy if part_name is None else f'{args.strategy}_{part_name}'
    out_dir = './output/%s/%s' % (args.dataset, folder_name) 
    if not os.path.isdir(out_dir):
        os.makedirs(out_dir)
    out_file = os.path.join(out_dir, 'passages.jsonl')
    return out_dir, out_file 

def main():
    args = get_args()
    block_id = 0

    out_dir = None
    out_file = None
    out_dir_cpr = None
    out_file_cpr = None
    out_dir_scm = None
    out_file_scm = None

    if args.strategy != 'cpr_scm':
        out_dir, out_file = get_out_file(args)
        if os.path.exists(out_file):
            answer = input('(%s) already exists. Continue?(y/n)' % out_file)
            if answer != 'y':
                return
    else:
        out_dir_cpr, out_file_cpr = get_out_file(args, part_name='compress')
        out_dir_scm, out_file_scm = get_out_file(args, part_name='schema')
        for data_file in [out_file_cpr, out_file_scm]:
            if os.path.exists(data_file):
                answer = input('(%s) already exists. Continue?(y/n)' % data_file)
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

    table_file_lst = get_table_files(args)
    arg_info_lst = []
    for task_id, table_file in enumerate(table_file_lst):
        arg_info = {
            'task_id':(task_id + 1),
            'args':args,
            'table_file':table_file,
            'out_dir':out_dir,
            'out_dir_cpr':out_dir_cpr,
            'out_dir_scm':out_dir_scm
        }
        arg_info_lst.append(arg_info)
    
    if not args.debug:
        num_workers = os.cpu_count()
        work_pool = ProcessPool(num_workers, initializer=init_worker, initargs=(args, ))
        task_batch_size = 1000
        num_tasks = len(arg_info_lst)
        for start_pos in range(0, num_tasks, task_batch_size):
            end_pos = min(start_pos + task_batch_size, num_tasks)
            batch_arg_lst = arg_info_lst[start_pos:end_pos]
            batch_task_num = len(batch_arg_lst)
            for serial_block_lst in tqdm(work_pool.imap_unordered(process_table, batch_arg_lst), total=batch_task_num):
                for serial_block in serial_block_lst:
                    serial_block['p_id'] = block_id
                    block_id += 1
                    do_write(args, f_o, f_o_cpr, f_o_scm, serial_block)
    else:
        init_worker(args)
        for arg_info in tqdm(arg_info_lst):
            serial_block_lst = process_table(arg_info)
            for serial_block in serial_block_lst:
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
