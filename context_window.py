import util

class ContextWindow:
    def __init__(self, tokenizer, wnd_size):
        self.tokenizer = tokenizer
        self.wnd_size = wnd_size
        self.content_size = 0
        self.content_buffer = []
        self.title = ''
        self.title_size = 0
        self.special_token_lst = []
        
    def set_title(self, title):
        self.title = title
        self.title_size = util.get_token_size(self.tokenizer, self.title)
        
    def add_special_tokens(self, special_token_lst):
        self.special_token_lst.extend(special_token_lst)

    def can_add(self, table_data, row, col_group_lst, row_serial_info):
        new_content_size = row_serial_info['content_size']
        if self.title_size + self.content_size + new_content_size > self.wnd_size:    
            return False
        return True
   
    def add(self, table_data, serial_info):
        self.content_size += serial_info['content_size']
        self.content_buffer.append(serial_info)

    def can_pop(self):
        return len(self.content_buffer) > 0

    def get_out_text(self):
        schema_refer_text_lst = []
        refer_text_lst = []
        cell_text_lst = []
        for serial_info in self.content_buffer:
            cell_lst = serial_info['cell_lst']
            cell_text_lst.extend([a['serial_text'] if a.get('schema', None) is None else a['schema'][0] + ' : ' + a['serial_text'] for a in cell_lst])
            code_info_lst = serial_info.get('code_info_lst', None)
            if code_info_lst:
                code_refer_lst = [a['code_refer'] for a in code_info_lst]
                refer_text_lst.extend(code_refer_lst)
            schema_code_info_lst = serial_info.get('schema_code_info_lst', None)
            if schema_code_info_lst:
                code_refer_lst = [a['code_refer'] for a in schema_code_info_lst]
                schema_refer_text_lst.extend(code_refer_lst)

        refer_text = ''.join(schema_refer_text_lst) +  ''.join(refer_text_lst)
        cell_text = ''.join(cell_text_lst)
        out_text = self.title + refer_text + cell_text
        return out_text

    def pop(self, table_data):
        assert(len(self.content_buffer) > 0)
        out_text = self.get_out_text()
        out_size = self.title_size + self.content_size
        out_data = {
            'passage':out_text,
            'tag':{
                'size':out_size,
                'table_id':table_data['tableId'],
                'row':[a['row'] for a in self.content_buffer],
                'cols':[a['cols'] for a in self.content_buffer],
                'special_tokens':self.special_token_lst,
            }
        }
        #comment out later
        out_size_re_calc = len(self.tokenizer.encode(out_text, add_special_tokens=False))
        assert(out_size_re_calc == out_size)
        assert(out_size <= self.wnd_size)
        self.clear_content()
        return out_data

    def clear_content(self):
        self.content_size = 0
        self.content_buffer = []
        self.special_token_lst = []
