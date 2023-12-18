import util

class ContextWindow:
    def __init__(self, tokenizer, wnd_size, stride=1):
        self.tokenizer = tokenizer
        self.wnd_size = wnd_size
        self.buffer_size = 0
        self.text_buffer = []
        self.schema_text = ''
        self.schema_size = 0

    def set_schema_text(self, text):
        self.schema_text = text
        self.schema_size = util.get_token_size(self.tokenizer, self.schema_text)

    def can_add(self, table_data, row_idx, col_idx, serial_text):
        cell_info = table_data['rows'][row_idx]['cells'][col_idx]
        col_data = table_data['columns']
        col_info = col_data[col_idx]
        is_last_cell = (col_idx + 1 == len(col_data))
        token_size = util.get_token_size(self.tokenizer, serial_text)
        cell_info['serial_text'] = serial_text
        cell_info['serial_size'] = token_size
        cell_info['is_last_cell'] = int(is_last_cell)
        cell_info['row'] = row_idx
        cell_info['col'] = col_idx
        
        if self.schema_size + self.buffer_size + token_size > self.wnd_size:
            return False
        return True
   
    def add(self, table_data, row_idx, col_idx):
        cell_info = table_data['rows'][row_idx]['cells'][col_idx]
        token_size = cell_info['serial_size']
        self.buffer_size += token_size
        self.text_buffer.append(cell_info)
    
    def can_pop(self):
        return len(self.text_buffer) > 0

    def pop(self, table_data):
        assert(len(self.text_buffer) > 0)
        text_lst = [a['serial_text'] for a in self.text_buffer]
        out_text = self.schema_size + ''.join(text_lst)
        out_size = self.schema_text + self.buffer_size
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


