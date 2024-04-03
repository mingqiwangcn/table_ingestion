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
            item_ctx_lst = item['ctxs']
            num_top_passages, top_table_lst = get_top_tables(item_ctx_lst, args.top)
            retr_metric = max([int(a in table_answer_set) for a in top_table_lst])
            retr_metric_lst.append(retr_metric)
            out_passage_lst = []
            for offset in range(num_top_passages):
                entry_info = item_ctx_lst[offset]
                entry_passage = entry_info['text']
                entry_table = entry_info['tag']['table_id']
                entry_correct = int(entry_table in table_answer_set)
                entry_rows = entry_info['tag']['row']
                passge_info = {
                    'passage':entry_passage,
                    'table_id':entry_table,
                    'correct':entry_correct,
                    'row':entry_rows,
                }
                out_passage_lst.append(passge_info)
            out_item = {
                'id':item['id'],
                'question':item['question'], 
                'table_id_lst':item['table_id_lst'],
                'top_correct':int(top_table_lst[0] in table_answer_set),
                'passages':out_passage_lst,
            }
            f_o.write(json.dumps(out_item) + '\n')
        precision = np.mean(retr_metric_lst) * 100
    f_o.close()
    print('%s precision@%d %.2f' % (args.strategy, args.top, precision))
    print(f'output retr {out_top_retr_file}')

def get_top_tables(ctx_lst, num_top_tables):
    table_lst = []
    table_set = set()
    num_top_passages = 0
    for ctx_info in ctx_lst:
        table_id = ctx_info['tag']['table_id']
        if table_id not in table_set:
            if len(table_set) == num_top_tables:
                break
            table_lst.append(table_id)
            table_set.add(table_id)
        num_top_passages += 1
    return num_top_passages, table_lst


if __name__ == '__main__':
    main()
