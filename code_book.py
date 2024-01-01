import util

class CodeBook:
    def __init__(self, tokenizer):
        self.tokenizer = tokenizer
        self.reset()

    def get_code(self, cell_info):
        text = cell_info['text']
        key = util.get_hash_key(text)
        if key not in self.text_dict:
            self.text_dict[key] = {'count':0, 'first_cell':cell_info}
        text_info = self.text_dict[key]
        text_info['count'] += 1
        if text_info['count'] == 2:
            code = '[E' + str(len(self.code_dict)+1) + ']'
            self.tokenizer.add_tokens([code], special_tokens=True)
            self.code_dict[key] = {'code':code}
            compress_code = code + ' is ' + text + ' ' + self.tokenizer.sep_token + ' ' 
            cell_info['compress_code'] = compress_code
            cell_info['first_cell'] = text_info['first_cell']

        if key in self.code_dict:
            code = self.code_dict[key]['code']
        else:
            code = text
        return code
    
    def reset(self):
        self.text_dict = {}
        self.code_dict = {}
        self.code_text = ''
        self.code_size = 0

