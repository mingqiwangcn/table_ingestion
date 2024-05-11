import argparse
import os
import generate_passage_embeddings as passage_encoder
import util

EmbFileTag = '_embeddings'

def get_encoder_args(model_path, is_student):
    encoder_args = argparse.Namespace(is_student=is_student,
                                      passages=None,
                                      output_path=None,
                                      output_batch_size=500000,
                                      shard_id=0,
                                      num_shards=1,
                                      per_gpu_batch_size=1000,
                                      passage_maxlength=util.Max_Seq_Length,
                                      model_path=model_path,
                                      no_fp16=False,
                                      show_progress=False
                                     )
    return encoder_args

def encode_blocks(args):
    block_file = f'./output/{args.dataset}/{args.strategy}/passages.jsonl'
    print('Encoding %s' % block_file)
    
    if args.is_student:
        encoder_model = os.path.join(args.work_dir, 'models/student_tqa_retriever_step_29500')
    else:
        encoder_model = os.path.join(args.work_dir, 'models/tqa_retriever')
    out_emb_file_lst = []
    encoder_args = get_encoder_args(encoder_model, args.is_student)
    encoder_args.passages = block_file
    passage_dir = os.path.dirname(block_file)
    base_name = os.path.basename(block_file)
    emb_dir = os.path.join(passage_dir, 'emb')
    if not os.path.isdir(emb_dir):
        os.makedirs(emb_dir)
    encoder_args.output_path = os.path.join(emb_dir, base_name + EmbFileTag)
    passage_encoder.main(encoder_args, is_main=False)

def main():
    args = get_args()
    encode_blocks(args)

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--work_dir', type=str, required=True)
    parser.add_argument('--dataset', type=str, required=True)
    parser.add_argument('--is_student', type=int, required=True)
    parser.add_argument('--strategy', type=str, required=True)
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    main()
