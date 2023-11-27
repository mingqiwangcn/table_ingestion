class ContextWindow:
    def __init__(self, tokenizer, wnd_size, stride=1):
        self.tokenizer = tokenizer
        self.wnd_size = wnd_size
        self.buffer_size = 0
        self.text_bufer = []
    
    def get_token_size(self, text):
        tokens = self.tokenizer.tokenize(text)
        return len(tokens)
    
    def can_add(self, table_data, col_idx, cell_info):
        col_data = table_data['cols']
        col_info = col_data[col_idx]
        is_last_cell = (col_idx + 1 == len(col_data))
        sep_token = ',' if not is_last_cell else '.'
        text = col_info['text'] + ' ' + cell_info['text'] + ' ' + sep_token + ' '
        token_size = self.get_token_size(text)
        cell_info['serial_text'] = text
        cell_info['serial_size'] = token_size
        title_prefix_size = table_data['title_size'] + 1 
        if title_prefix_size + self.buffer_size + token_size > self.wnd_size:
            return False
        return True
    
    def add(self, cell_info):
        token_size = cell_info['serial_size']
        self.buffer_size += token_size
        self.text_buffer.append(cell_info)

    def pop(self, table_data):
        assert(len(self.text_bufer) > 0)
        text_lst = [a['serial_text'] for a in self.text_bufer]
        out_text = table_data['document_title'] + '.' + ' '.join(text_lst)
        out_size = table_data['title_size'] + 1 + self.buffer_size
        out_data = {
            'text':out_text,
            'size':out_size
        }
        #comment out later
        assert(len(self.tokenizer.tokenize(out_text)) <= self.wnd_size)
        
        self.reset()
        return out_data

    def reset(self):
        self.buffer_size = 0
        self.text_bufer = []


