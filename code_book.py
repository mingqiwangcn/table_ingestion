import util

class CodeBook:
    def __init__(self, tokenizer):
        self.tokenizer = tokenizer
        self.reset()

    # Let x is the size of cell text, y is the # of occurences of the cell text. Since the compressed text inlcude [E?] y times and also the text "[E?] is {cell text} [SEP]", 
    # we must have yx > y + x + 3, so x > (y+3) / (y-1) = 1 + ( 4 / (y-1) ), which is the condition to start compression. We must tract those cells before the condition is stisfied, so that
    # we can update them with codes
    def get_code(self, cell_info):
        text = cell_info['text']
        key = util.get_hash_key(text)
        if key not in self.text_dict:
            self.text_dict[key] = {'count':0, 'pre_cells':[]}
        text_info = self.text_dict[key]
        text_info['count'] += 1
        
        x = cell_info['size']
        y = text_info['count']

        if key in self.code_dict:
            code = self.code_dict[key]['code']
            return code, 1

        if y > 1 and ( x > 1 + ( 4 / (y - 1) ) ): 
            code = '[E' + str(len(self.code_dict)+1) + ']'
            if code not in self.special_token_dict:
                self.special_token_dict[code] = True
            self.tokenizer.add_tokens([code], special_tokens=True)
            self.code_dict[key] = {'code':code}
            compress_code = code + ' is ' + text + ' ' + self.tokenizer.sep_token + ' ' 
            cell_info['compress_code'] = compress_code
            cell_info['cpr_code_size'] = 1 + 1 + cell_info['size'] + 1
            cell_info['pre_cells'] = text_info['pre_cells']

            return code, 1
        else:
            pre_cell_lst = text_info['pre_cells']
            pre_cell_lst.append(cell_info)
            return text, cell_info['size']  

    def reset(self):
        self.text_dict = {}
        self.special_token_dict = {}
        self.code_dict = {}
        self.code_text = ''
        self.code_size = 0

