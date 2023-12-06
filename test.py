import json
from tqdm import tqdm
import os
from serial import BlockSerializer
from multiprocessing import Pool as ProcessPool

def read_tables():
    data_file = '/home/cc/code/solo_work/data/chicago_open/tables/tables.jsonl'
    table_lst = []
    with open(data_file) as f:
        for line in tqdm(f):
            table_data = json.loads(line)
            table_lst.append(table_data)
    return table_lst

def init_worker():
    global tsl
    tsl = BlockSerializer()

def process_table(arg_info):
    table_data = arg_info['table_data']
    block_lst = []
    for serial_block in tsl.serialize(table_data):
        block_lst.append(serial_block)
    return block_lst

def main():
    block_id = 0
    out_file = './data/chicago_open/passages.jsonl'
    if os.path.exists(out_file):
        answer = input('(%s) already exists. Continue?(y/n)' % out_file)
        if answer != 'y':
            return
    f_o = open(out_file, 'w')

    print('Loading tables')
    table_lst = read_tables()
    arg_info_lst = []
    for table_data in table_lst:
        arg_info = {
            'table_data':table_data,
        }
        arg_info_lst.append(arg_info)

    work_pool = ProcessPool(initializer=init_worker)
    
    for table_block_lst in tqdm(work_pool.imap_unordered(process_table, arg_info_lst), total=len(table_lst)):
        for serial_block in table_block_lst:
            serial_block['p_id'] = block_id
            block_id += 1
            f_o.write(json.dumps(serial_block) + '\n')

    f_o.close()

if __name__ == '__main__':
    main()
