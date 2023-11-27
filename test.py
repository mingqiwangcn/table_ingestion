from serial import TableSerializer
import json

def read_tables():
    data_file = '/home/cc/code/solo_work/data/nyc_open_1000/tables/tables.jsonl'
    with open(data_file) as f:
        for line in f:
            table_data = json.loads(line)
            yield table_data

def main():
    tsl = TableSerializer()
    for table_data in read_tables():
        for serial_chunk in tsk.serialize(table_data):


if __name__ == '__main__':
    main()
