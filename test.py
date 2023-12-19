import json
from tqdm import tqdm
import os
from serial_block import BlockSerializer
from serial_schema import SchemaSerializer 
from multiprocessing import Pool as ProcessPool
import argparse

def read_tables(args):
    data_file = '../data/%s/tables/tables.jsonl' % args.dataset
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
    else:
        raise ValueError('Strategy (%s) not supported.' % args.strategy)

def process_table(arg_info):
    table_data = arg_info['table_data']
    block_lst = []
    for serial_block in tsl.serialize(table_data):
        block_lst.append(serial_block)
    return block_lst

def main():
    args = get_args()
    block_id = 0
    out_dir = './output/%s/%s' % (args.dataset, args.strategy)
    if not os.path.isdir(out_dir):
        os.makedirs(out_dir)
    out_file = os.path.join(out_dir, 'passages.jsonl')
    if os.path.exists(out_file):
        answer = input('(%s) already exists. Continue?(y/n)' % out_file)
        if answer != 'y':
            return

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

    f_o = open(out_file, 'w')
    if not args.debug:
        work_pool = ProcessPool(initializer=init_worker, initargs=(args, ))
        for table_block_lst in tqdm(work_pool.imap_unordered(process_table, arg_info_lst), total=len(table_lst)):
            for serial_block in table_block_lst:
                serial_block['p_id'] = block_id
                block_id += 1
                f_o.write(json.dumps(serial_block) + '\n')
    else:
        init_worker(args)
        for arg_info in tqdm(arg_info_lst):
            table_block_lst = process_table(arg_info)
            for serial_block in table_block_lst:
                serial_block['p_id'] = block_id
                block_id += 1
                f_o.write(json.dumps(serial_block) + '\n')
            
    f_o.close()

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', type=str, required=True)
    parser.add_argument('--strategy', type=str, required=True)
    parser.add_argument('--debug', type=int)
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()
