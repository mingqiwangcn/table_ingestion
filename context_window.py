import util

class ContextWindow:
    def __init__(self, tokenizer, wnd_size):
        self.tokenizer = tokenizer
        self.wnd_size = wnd_size
        self.buffer_size = 0
        self.text_buffer = []
        self.schema_text = ''
        self.schema_size = 0
        self.cell_code_book = None

    def set_cell_code_book(self, cell_code_book):
        self.cell_code_book = cell_code_book

    def set_schema_text(self, text):
        self.schema_text = text
        self.schema_size = util.get_token_size(self.tokenizer, self.schema_text)

    def can_add(self, table_data, row, col_group_lst, serial_text, serial_size):
        row_cells = table_data['rows'][row]['cells']
        col_data = table_data['columns']
        
        serial_info = {}
        serial_info['text'] = serial_text
        serial_info['size'] = serial_size
        serial_info['row'] = row
        serial_info['cols'] = col_group_lst
       
        code_size = 0
        updated_buffer_size = self.buffer_size
        
        if self.cell_code_book is not None:
            cpr_code_size = 0
            for col_group in col_group_lst:
                first_col = col_group[0]
                cell_info = row_cells[first_col]
                compress_code = cell_info.get('compress_code', None)
                if compress_code is not None:
                    cpr_code_size = cell_info['cpr_code_size']
                    pre_cell_lst = cell_info['pre_cells']
                    for pre_cell in pre_cell_lst:
                        pre_cell_size_chg = pre_cell['updated_serial_size'] - pre_cell['serial_size']                
                        updated_buffer_size += self.buffer_size + pre_cell_size_chg 

            code_size = self.cell_code_book.code_size + cpr_code_size 

        if self.schema_size + code_size + updated_buffer_size + serial_size > self.wnd_size:
            for col_group in col_group_lst:
                first_col = col_group[0]
                cell_info = row_cells[first_col]
                compress_code = cell_info.get('compress_code', None)
                if compress_code is not None:
                    del cell_info['compress_code']
                    pre_cell_lst = cell_info['pre_cells']
                    for pre_cell in pre_cell_lst:
                        del pre_cell['updated_serial_text']
                        del pre_cell['updated_serial_size']
                    del cell_info['pre_cells']
            return False, serial_info
        
        return (True, serial_info)
   
    def add(self, table_data, serial_info):
        self.buffer_size += serial_info['size']
        self.text_buffer.append(serial_info)
        row = serial_info['row']
        col_group_lst = serial_info['cols']
        row_cells = table_data['rows'][row]['cells']
        for col_group in col_group_lst:
            first_col = col_group[0]
            cell_info = row_cells[first_col]
            compress_code = cell_info.get('compress_code', None)
            if compress_code is not None:
                pre_cell_lst = cell_info['pre_cells']
                for pre_cell in pre_cell_lst:
                    pre_cell_size_chg = pre_cell['updated_serial_size'] - pre_cell['serial_size']                
                    self.buffer_size += pre_cell_size_chg
                    pre_cell['serial_text'] = pre_cell['updated_serial_text']
                    pre_cell['serial_size'] = pre_cell['updated_serial_size']
                
                self.cell_code_book.code_text += compress_code
                self.cell_code_book.code_size += cell_info['cpr_code_size']

    def can_pop(self):
        return len(self.text_buffer) > 0

    def pop(self, table_data):
        assert(len(self.text_buffer) > 0)
        text_lst = [a['text'] for a in self.text_buffer]
        code_text = ''
        code_size = 0
        special_token_lst = None
        if self.cell_code_book is not None:
            code_text = self.cell_code_book.code_text
            code_size = self.cell_code_book.code_size
            special_token_lst = list(self.cell_code_book.special_token_dict.keys())

        out_text = self.schema_text + code_text + ''.join(text_lst)
        out_size = self.schema_size + code_size + self.buffer_size
        out_data = {
            'passage':out_text,
            'tag':{
                'size':out_size,
                'table_id':table_data['tableId'],
                'row':[a['row'] for a in self.text_buffer],
                'cols':[a['cols'] for a in self.text_buffer],
                'special_tokens':special_token_lst,
            }
        }
        #comment out later
        #assert(len(self.tokenizer.tokenize(out_text)) <= self.wnd_size)
        
        self.reset()
        return out_data

    def reset(self):
        self.buffer_size = 0
        self.text_buffer = []
        if self.cell_code_book is not None:
            self.cell_code_book.reset()
        

