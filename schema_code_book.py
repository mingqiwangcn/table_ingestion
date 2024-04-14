import util
class SchemaCodeBook:
    def __init__(self, tokenizer):
        self.tokenizer = tokenizer
        self.reset()

    def get_code(self, col_data, col, cell_info):
        text = cell_info[text_key]
        col_key = col
        if col_key not in self.code_dict:
            self.code_dict[col_key] = {'count':0, 'code':None, 'pre_cells':[]}
        code_info = self.code_dict[col_key]
        code_info['count'] += 1
        
        column_info = col_data[col]
        x = column_info['size']
        y = code_info['count']

        code = self.code_dict[col_key]['code']
        if code is not None:
            return code, 1
        col_text = column_info['text']
        col_size = column_info['size']
        if y > 1 and ( x > 1 + ( 4 / (y - 1) ) ): 
            code = '[C' + str(col) + ']'
            if code not in self.special_token_dict:
                self.special_token_dict[code] = True
            self.tokenizer.add_tokens([code], special_tokens=True)
            self.code_dict[col_key]['code'] = code
            compress_code = code + ' is ' + col_text + ' ' + self.tokenizer.sep_token + ' ' 
            cell_info['compress_code'] = compress_code
            cell_info['cpr_code_size'] = 1 + 1 + col_size + 1
            cell_info['pre_cells'] = code_info['pre_cells']
            return code, 1
        else:
            pre_cell_lst = code_info['pre_cells']
            pre_cell_lst.append(cell_info)
            return col_text, col_size

    def reset(self):
        self.code_dict = {}
        self.code_text = ''
        self.code_size = 0
        self.special_token_dict = {}
        