
class SerialCollator:
    def __init__(self, tokenizer):
        self.tokenizer = tokenizer

    def get_text_ids(self, text_lst):
        encode_output = self.tokenizer.batch_encode_plus(
            text_lst,
            return_tensors='pt',
            max_length=512,
            pad_to_max_length=True,
            truncation=True,
        )
        return encode_output['input_ids'] 

    def get_schem_text(self, schema_info):
        text = schema_info['title'] + ' . ' + schema_info['col_text']
        return text

    def __call__(self, batch_passage_info):
        passge_lst = [a['passage'] for a in batch_passage_info]
        passage_text_ids = self.get_text_ids(passge_lst)
        schema_text_lst = [self.get_schem_text(a['schema_info']) for a in batch_passage_info]
        schema_text_ids = self.get_text_ids(schema_text_lst)
        return passage_text_ids, schema_text_ids