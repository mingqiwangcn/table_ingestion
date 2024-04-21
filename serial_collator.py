
class SerialCollator:
    def __init__(self, tokenizer):
        self.tokenizer = tokenizer

    def __call__(self, batch_passage_info):
        passge_lst = [a['passage'] for a in batch_passage_info]
        encode_output = self.tokenizer.batch_encode_plus(
            passge_lst,
            return_tensors='pt',
            max_length=512,
            pad_to_max_length=True,
            truncation=True,
            
        )
        return encode_output['input_ids']