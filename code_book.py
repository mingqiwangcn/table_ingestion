import util

class CodeBook:
    def __init__(self, tokenizer):
        self.tokenizer = tokenizer
        self.reset()

    # Let x is the size of cell text, y is the # of occurences of the cell text. 
    # Since the compressed text inlcude [E?] y times and also the text "[E?] is {cell text} [SEP]", 
    # we must have yx > y + x + 3, so x > (y+3) / (y-1) = 1 + ( 4 / (y-1) ), 
    # which is the condition to start compression. We must tract those cells before the condition is stisfied, so that
    # we can update them with codes
    def get_code(self, row, col, cell_info, text_key='text', size_key='size'):
        text = cell_info[text_key]
        key = util.get_hash_key(text)
        if key not in self.code_dict:
            self.code_dict[key] = {'count':0, 'code':None, 
                                   'code_refer':None, 'code_refer_size':None, 
                                   'pre_cells':[], 'cpr_start_cell':None}
        code_info = self.code_dict[key]
        code_info['count'] += 1
        
        text_size = cell_info[size_key]
        x = text_size
        y = code_info['count']

        code = code_info['code']
        if code is not None:
            return code, 1

        if y > 1 and ( x > 1 + ( 4 / (y - 1) ) ):
            self.code_number += 1
            code = '[E' + str(self.code_number) + ']'
            if code not in self.special_token_dict:
                self.special_token_dict[code] = True
            self.tokenizer.add_tokens([code], special_tokens=True)
            code_info['code'] = code
            code_refer = code + ' is ' + text + ' ' + self.tokenizer.sep_token + ' ' 
            code_info['code_refer'] = code_refer
            code_info['code_refer_size'] = 1 + 1 + text_size + 1
            code_info['cpr_start_cell'] = cell_info
            cell_info['code_info'] = code_info
            return code, 1
        else:
            pre_cell_lst = code_info['pre_cells']
            cell_info['row'] = row
            cell_info['col'] = col
            pre_cell_lst.append(cell_info)
            return text, text_size

    def reset(self):
        self.code_number = 0
        self.code_dict = {}
        self.code_text = ''
        self.code_size = 0
        self.special_token_dict = {}
        
