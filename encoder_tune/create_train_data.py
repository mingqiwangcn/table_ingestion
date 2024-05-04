import json
import os
from tqdm import tqdm

def read_tables(args):
    file_pattern = os.path.join(args.sample_dir, 'tables/*.jsonl')
    table_file_lst = glob.glob(file_pattern)
    table_dict = {}
    table_number_map = {}
    for table_file in tqdm(table_file_lst):
        with open(table_file) as f:
            for line in f:
                table_data = json.loads(line)
                table_id = table_data['tableId']
                table_number = table_data['table_number']
                table_number_map[table_number] = table_id
                if table_id not in table_dict:
                    table_dict[table_id] = []
                table_sub_lst = table_dict[table_id]
                table_sub_lst.append(table_data)
    return table_dict, table_number_map

def read_questions(args):
    question_file = os.path.join(args.sample_dir, 'questions.jsonl')
    q_info_lst = []
    with open(question_file) as f:
        for line in tqdm(f):
            q_info = json.loads(line)
            q_info_lst.append(q_info)
    return q_info_lst

def main():
    args = get_args()
    work_dir = os.path.dirname(os.getcwd())
    args.work_dir = work_dir
    args.sample_dir = os.path.join(work_dir, 'output', args.dataset, args.strategy, 'samples')
    table_dict, table_number_map = read_tables(args)
    q_info_lst = read_questions(args)
    example_lst = []
    for q_id, q_info in enumerate(q_info_lst):
        q_table_number = q_info['table_number']
        example = {
            'id':q_id,
            'q_id':q_id,
            'question':q_info['question'],
            'target':'N/A',
            'ctx':[]
        }
        pos_ctx = {
            'title':
        }
        ctx_lst = example['ctx']



def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', type=str, required=True)
    parser.add_argument('--strategy', type=str, required=True)
    parser.add_argument('--model', type=str)
    parser.add_argument('--batch_size', type=int, default=32)
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()