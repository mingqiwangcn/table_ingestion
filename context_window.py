import util

class ContextWindow:
    def __init__(self, tokenizer, wnd_size):
        self.tokenizer = tokenizer
        self.wnd_size = wnd_size
        self.content_size = 0
        self.content_buffer = []
        self.title = ''
        self.title_size = 0
    
    def set_title(self, title):
        self.title = title
        self.title_size = util.get_token_size(self.tokenizer, self.title)

    def can_add(self, table_data, row, col_group_lst, row_serial_info):
        cur_content_size = row_serial_info['content_size']
        if self.title_size + self.content_size + cur_content_size > self.wnd_size:    
            return False
        return True
   
    def add(self, table_data, serial_info):
        self.content_size += serial_info['content_size']
        self.content_buffer.append(serial_info)

    def can_pop(self):
        return len(self.text_buffer) > 0

    def pop(self, table_data):
        assert(len(self.content_buffer) > 0)
        refer_text_lst = []
        cell_text_lst = []
        for serial_info in self.content_buffer:
            cell_lst = serial_info['cell_lst']
            cell_text_lst.extend([a['serial_text'] for a in cell_lst])
            code_refer_lst = serial_info['code_refer_lst']
            refer_text_lst.extend(code_refer_lst)

        refer_text = ''.join(refer_text_lst)
        cell_text = ''.join(cell_text_lst)
        out_text = self.title + refer_text + cell_text
        out_size = self.content_size
        out_data = {
            'passage':out_text,
            'tag':{
                'size':out_size,
                'table_id':table_data['tableId'],
                'row':[a['row'] for a in self.content_buffer],
                'cols':[a['cols'] for a in self.content_buffer],
                'special_tokens':special_token_lst,
            }
        }
        #comment out later
        assert(len(self.tokenizer.tokenize(out_text)) == out_size)
        assert(out_size <= self.wnd_size)

        self.clear_content()
        return out_data

    def clear_content(self):
        self.content_size = 0
        self.content_buffer = []

