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

    def set_cell_code_book(cell_code_book):
        self.cell_code_book = cell_code_book

    def set_schema_text(self, text):
        self.schema_text = text
        self.schema_size = util.get_token_size(self.tokenizer, self.schema_text)

    def can_add(self, table_data, row_idx, col_idx, serial_text):
        cell_info = table_data['rows'][row_idx]['cells'][col_idx]
        col_data = table_data['columns']
        col_info = col_data[col_idx]
        #is_last_cell = (col_idx + 1 == len(col_data))
        token_size = util.get_token_size(self.tokenizer, serial_text)
        cell_info['serial_text'] = serial_text
        cell_info['serial_size'] = token_size
        #cell_info['is_last_cell'] = int(is_last_cell)
        cell_info['row'] = row_idx
        cell_info['col'] = col_idx
       
        code_size = 0
        updated_buffer_size = self.buffer_size
        compress_code = cell_info.get('compress_code', None)
        if self.cell_code_book is not None:
            cpr_code_size = 0
            if compress_code is not None:
                cpr_code_size = util.get_token_size(compress_code)
                cell_info['cpr_code_size'] = cpr_code_size
                first_cell = cell_info['first_cell']
                first_cell_size_chg = first_cell['updated_serial_size'] - first_cell['serial_size']                
                updated_buffer_size = self.buffer_size + first_cell_size_chg 

            code_size = self.cell_code_book.code_size + cpr_code_size 

        if self.schema_size + code_size + updated_buffer_size + token_size > self.wnd_size:
            if compress_code is not None:
                del cell_info['compress_code']
                first_cell = cell_info['first_cell']
                del first_cell['updated_serial_text']
                del first_cell['updated_serial_size']
                del cell_info['first_cell']
            return False
        return True
   
    def add(self, table_data, row_idx, col_idx):
        cell_info = table_data['rows'][row_idx]['cells'][col_idx]
        token_size = cell_info['serial_size']
        self.buffer_size += token_size
        self.text_buffer.append(cell_info)
        compress_code = cell_info.get('compress_code', None)
        if compress_code is not None:
            first_cell = cell_info['first_cell']
            first_cell_size_chg = first_cell['updated_serial_size'] - first_cell['serial_size']                
            self.buffer_size += first_cell_size_chg
            first_cell['serial_text'] = first_cell['updated_serial_text']
            first_cell['serial_size'] = first_cell['updated_serial_size']
            self.cell_code_book.code_text += compress_code
            self.cell_code_book.code_size += cell_info['cpr_code_size']

    def can_pop(self):
        return len(self.text_buffer) > 0

    def pop(self, table_data):
        assert(len(self.text_buffer) > 0)
        text_lst = [a['serial_text'] for a in self.text_buffer]
        out_text = self.schema_text + ''.join(text_lst)
        out_size = self.schema_size + self.buffer_size
        out_data = {
            'passage':out_text,
            'tag':{
                'size':out_size,
                'table_id':table_data['tableId'],
                'row':[a['row'] for a in self.text_buffer],
                'col':[a['col'] for a in self.text_buffer],
                'is_last_cell':[a['is_last_cell'] for a in self.text_buffer]
            }
        }
        #comment out later
        assert(len(self.tokenizer.tokenize(out_text)) <= self.wnd_size)
        
        self.reset()
        return out_data

    def reset(self):
        self.buffer_size = 0
        self.text_buffer = []

