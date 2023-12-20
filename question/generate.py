import argparse
from tqdm import tqdm
import json
from chatgpt_questions import ChatGptGenerator

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', type=str, required=True)
    args = parser.parse_args()
    return args

def read_tables(args):
    data_file = '../../data/%s/tables/tables.jsonl' % args.dataset
    table_lst = []
    with open(data_file) as f:
        for line in tqdm(f):
            table_data = json.loads(line)
            if len(table_data['rows']) == 0:
                continue
            yield table_data

def main():
    args = get_args()
    table_iterator = read_tables(args)
    generator = ChatGptGenerator('./prompt')
    generator.generate_questions(table_iterator)

if __name__ == '__main__':
    main()
