import util
class SchemaCodeBook:
    def __init__(self, tokenizer):
        self.tokenizer = tokenizer
        self.reset()

    def get_code(self, col_data, col, cell_info):
        col_key = col
        if col_key not in self.code_dict:
            self.code_dict[col_key] = {'count':0, 'code':None,
                                       'code_refer':None, 'code_refer_size':None, 
                                       'pre_cells':[], 'cpr_start_cell':None}
        code_info = self.code_dict[col_key]
        code_info['count'] += 1
        
        column_info = col_data[col]
        col_text = column_info['text']
        col_size = column_info['size']
        x = col_size
        y = code_info['count']

        code = self.code_dict[col_key]['code']
        if code is not None:
            return code, 1
        
        if y > 1 and ( x > 1 + ( 4 / (y - 1) ) ): 
            code = '[C' + str(col) + ']'
            if code not in self.special_token_dict:
                self.special_token_dict[code] = True
            self.tokenizer.add_tokens([code], special_tokens=True)
            code_info['code'] = code
            code_refer = code + ' is ' + col_text + ' ' + self.tokenizer.sep_token + ' '
            code_info['code_refer'] = code_refer
            code_info['code_refer_size'] = 1 + 1 + col_size + 1
            code_info['cpr_start_cell'] = cell_info
            cell_info['schema_code_info'] = code_info
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
        