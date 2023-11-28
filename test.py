import json
from tqdm import tqdm
import os
from serial import TableSerializer

def read_tables():
    data_file = '/home/cc/code/solo_work/data/nyc_open_1000/tables/tables.jsonl'
    with open(data_file) as f:
        for line in f:
            table_data = json.loads(line)
            yield table_data

def main():
    tsl = TableSerializer()
    block_id = 0
    out_file = './output/nyc_1000_passages.jsonl'
    if os.path.exists(out_file):
        answer = input('(%s) already exists. Continue?(y/n)' % out_file)
        if answer != 'y':
            return
    f_o = open(out_file, 'w')

    for table_data in tqdm(read_tables()):
        for serial_block in tsl.serialize(table_data):
            serial_block['p_id'] = block_id
            block_id += 1
            f_o.write(json.dumps(serial_block) + '\n')

    f_o.close()

if __name__ == '__main__':
    main()
