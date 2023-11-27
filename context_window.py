class ContextWindow:
    def __init__(self, tokenizer, wnd_size, stride=1):
        self.tokenizer = tokenizer
        self.wnd_size = wnd_size
        self.cur_size = 0
        self.text_bufer = []
    
    def get_token_size(self, text):
        tokens = self.tokenizer.tokenize(text)
        return len(tokens)
    
    def can_add(self, col_info, cell_info):
        text = col_info['text'] + '  ' + cell_info['text'] + ' ,'
        token_size = self.get_token_size(text)
        cell_info['cw_size'] = token_size
        if token_size + self.cur_size > self.wnd_size:
            return False
        return True
    
    def add(self, row_idx, col_idx, cell_info):
        token_size = cell_info['cw_size']
        self.cur_size += token_size
        self.text_buffer.append(cell_info)
