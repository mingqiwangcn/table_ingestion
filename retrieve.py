import os
import argparse
import json
from trainer import read_config, read_tables, retr_triples

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--work_dir', type=str, required=True)
    parser.add_argument('--dataset', type=str, required=True)
    parser.add_argument('--strategy', type=str, required=True)
    parser.add_argument('--n_probe', type=int, required=True)
    args = parser.parse_args()
    return args

def main():
    args = get_args()
    config = read_config()
    test_query_dir = os.path.join(args.work_dir, 'data', args.dataset, 'query', 'test')
    table_dict = read_tables(args.work_dir, args.dataset)
    retr_triples('test', args.work_dir, args.dataset, 
                 test_query_dir, table_dict, False, 
                 config, strategy=args.strategy, use_tag=False, 
                 n_probe=args.n_probe)

if __name__ == '__main__':
    main()

