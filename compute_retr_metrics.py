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
    out_retr_dir = os.path.join(args.work_dir, 'data', args.dataset, 
                            'query', 'test', args.strategy, 
                            'exact')
    retr_file = os.path.join(out_retr_dir, 'fusion_retrieved.jsonl')
    out_top_retr_file = os.path.join(out_retr_dir, f'retr_{args.strategy}_top_{args.top}.jsonl')
    f_o = open(out_top_retr_file, 'w')
    with open(retr_file) as f:
        retr_metric_lst = []
        for line in f:
            item = json.loads(line)
            table_answer_set = set(item['table_id_lst'])
            top_table_lst, top_table_dict = get_top_tables(item['ctxs'], args.top, table_answer_set)
            retr_metric = max([top_table_dict[a]['correct'] for a in top_table_lst])
            retr_metric_lst.append(retr_metric)
            out_item = {
                'id':item['id'],
                'question':item['question'], 
                'table_answers':item['table_id_lst'],
                'top_tables':top_table_lst,
                'table_data':top_table_dict
            }
            f_o.write(json.dumps(out_item) + '\n')
        precision = np.mean(retr_metric_lst) * 100
    f_o.close()
    print('%s precision@%d %.2f' % (args.strategy, args.top, precision))
    print(f'output retr {out_top_retr_file}')

def get_top_tables(ctx_lst, num_top, table_answer_set):
    table_lst = []
    table_dict = {}
    for ctx_info in ctx_lst:
        table_id = ctx_info['tag']['table_id']
        if table_id not in table_dict:
            if len(table_dict) == num_top:
                break
            table_lst.append(table_id)
            correct = int(table_id in table_answer_set)
            table_dict[table_id] = {'correct':correct, 'ctxs':[]}
        table_ctx_lst = table_dict[table_id]['ctxs']
        table_ctx_lst.append(ctx_info)
    return table_lst, table_dict


if __name__ == '__main__':
    main()
