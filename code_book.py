import util

class CodeBook:
    def __init__(self, tokenizer):
        self.tokenizer = tokenizer
        self.cell_dict = {}
        self.code_dict = {}
        self.code_text = ''
        self.code_size = 0

    def get_code(self, cell_info):
        text = cell_info['text']
        key = util.get_hash_key(text)
        if key not in self.cell_dict:
            self.text_dict[key] = {'count':0, 'first_cell':cell_info}
        text_info = self.text_dict[key]
        text_info['count'] += 1
        if text_info['count'] == 2:
            code = '[e' + str(len(code_dict)+1) + ']'
            cls_token = 'cls_' + code 
            self.tokenizer.add_special_tokens({cls_token:code})
            self.code_dict[key] = {'code':code}
            compress_code = code + ' is ' + text + ' ' + self.tokenizer.sep_token + ' ' 
            cell_info['compress_code'] = compress_code
            cell_info['first_cell'] = text_info['first_cell']

        if key in self.code_dict:
            code = self.code_dict[key]['code']
        else:
            code = text
        return code
   
