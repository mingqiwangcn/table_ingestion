import json
import argparse
from tqdm import tqdm

def main():
    args = get_args()
    serial_file = f'./output/{args.dataset}/{args.strategy}/passages.jsonl'
    stat_dict = {}
    with open(serial_file) as f:
        for line in tqdm(f):
            item = json.loads(line)
            table_id = item['tag']['table_id']
            if table_id not in stat_dict:
                stat_dict[table_id] = {'count':0}
            stat_info = stat_dict[table_id]
            stat_info['count'] += 1
    
    out_file = f'./output/{args.dataset}/{args.strategy}/stat.json'
    with open(out_file, 'w') as f_o:
        json.dump(stat_dict, f_o)

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', type=str, required=True)
    parser.add_argument('--strategy', type=str, required=True)
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()
