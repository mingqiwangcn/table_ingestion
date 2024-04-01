import os
import argparse
import json
from trainer import read_config, retr_triples
import util

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--work_dir', type=str, required=True)
    parser.add_argument('--dataset', type=str, required=True)
    parser.add_argument('--strategy', type=str, required=True)
    parser.add_argument('--index_type', type=str)
    parser.add_argument('--n_probe', type=int)
    args = parser.parse_args()
    return args

def main():
    args = get_args()
    config = read_config()
    config['text_maxlength'] = util.Max_Seq_Length
    config['min_tables'] = 1
    assert args.index_type == 'exact'
    config['index_type'] = args.index_type
    config['exact_index_dir'] = os.path.abspath(f'./output/{args.dataset}/{args.strategy}/')
    config['query_batch'] = 1000

    test_query_dir = os.path.join(args.work_dir, 'data', args.dataset, 'query', 'test')
    if args.index_type is not None:
        out_dir = os.path.join(test_query_dir, args.strategy, args.index_type)
    else:
        out_dir = os.path.join(test_query_dir, args.strategy)
    if os.path.isdir(out_dir):
        print(f'{out_dir} already exists.')
        return
    retr_triples('test', args.work_dir, args.dataset, 
                 test_query_dir, None, False, 
                 config, strategy=args.strategy, use_tag=False)

if __name__ == '__main__':
    main()

