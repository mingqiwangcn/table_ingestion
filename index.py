import argparse
import glob
from src import ondisk_index

def get_index_args(work_dir, dataset, strategy):
    emb_file = 'passages.jsonl_embeddings_*'
    index_args = argparse.Namespace(work_dir=work_dir,
                                    dataset=dataset,
                                    experiment=strategy,
                                    emb_file=emb_file
                                    )
    return index_args

def main():
    args = get_args()
    emb_file_pattern = f'./output/{args.dataset}/{args.strategy}/emb/passages.jsonl_embeddings_*' 
    out_emb_file_lst = glob.glob(emb_file_pattern)
    if len(out_emb_file_lst) == 0:
        print('There is no embedding files')
        return
    print('\nCreating index')
    index_args = get_index_args(args.work_dir, args.dataset, args.strategy)
    msg_info = ondisk_index.main(index_args)
    if msg_info is not None:
        if not msg_info['state']:
            print(msg_info['msg'])
            return
    print('\nIndexing done')

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--work_dir', type=str)
    parser.add_argument('--dataset', type=str)
    parser.add_argument('--strategy', type=str)
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    main()

