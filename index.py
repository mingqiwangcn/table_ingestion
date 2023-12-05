import argparse
import glob
from src import ondisk_index

def get_index_args(work_dir, dataset):
    emb_file = 'passages.jsonl_embeddings_*'
    index_args = argparse.Namespace(work_dir=work_dir,
                                    dataset=dataset,
                                    experiment='block',
                                    emb_file=emb_file
                                    )
    return index_args

def main():
    emb_file_pattern = './data/chicago_open/emb/passages.jsonl_embeddings_*' 
    out_emb_file_lst = glob.glob(emb_file_pattern)

    if len(out_emb_file_lst) == 0:
        print('There is no embedding files')
        return

    print('\nCreating index')
    args = get_args()
    dataset = 'chicago_open'
    index_args = get_index_args(args.work_dir, dataset)
    msg_info = ondisk_index.main(index_args)

    print('\nIndexing done')

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--work_dir', type=str)
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    main()

