import json
import argparse
import os
import numpy as np

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--work_dir', type=str)
    parser.add_argument('--dataset', type=str)
    parser.add_argument('--strategy', type=str)
    parser.add_argument('--top', default=5, type=int)
    args = parser.parse_args()
    return args

def main():
    args = get_args()
    retr_file = os.path.join(args.work_dir, 'data', args.dataset, 
                             'query', 'test', args.strategy, 
                             'exact/fusion_retrieved.jsonl')
    
    with open(retr_file) as f:
        retr_metric_lst = []
        for line in f:
            item = json.loads(line)
            table_id_lst = item['table_id_lst']
            metric_info = {}
            for table_id in table_id_lst:
                metric_info[table_id] = {'correct':0}
            ctx_lst = get_top_tables(item['ctxs'], args.top)
            for ctx_info in ctx_lst:
                table_id = ctx_info['tag']['table_id']
                if table_id in metric_info:
                    metric_info[table_id]['correct'] = 1
            
            correct_lst = [metric_info[a]['correct'] for a in metric_info]
            retr_metric = np.mean(correct_lst)
            retr_metric_lst.append(retr_metric)

        recall = np.mean(retr_metric_lst) * 100
    print('%s recall@%d %.2f' % (args.strategy, args.top, recall))

def get_top_tables(ctx_lst, num_top):
    retr_ctx_lst = []
    table_set = set()
    for ctx_info in ctx_lst:
        table_id = ctx_info['tag']['table_id']
        if table_id not in table_set:
            if len(table_set) == num_top:
                break
            table_set.add(table_id)
        retr_ctx_lst.append(ctx_info)
    return retr_ctx_lst


if __name__ == '__main__':
    main()
