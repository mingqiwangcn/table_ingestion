import util
from context_window import ContextWindow

class State:
    def __init__(self):
        self.row = -1
        self.col = -1
    
    def update(row, col):
        self.row = row
        self.col = col

class TableSerializer:
    def __init__(self):
        self.tokenizer = transformers.BertTokenizerFast.from_pretrained('bert-base-uncased')
        self.progress_state = State()
        wnd_size = util.get_context_window_size(self.tokenizer)
        self.window = ContextWindow(self.tokenizer, wnd_size) 

    def serialize(table_data):
        util.preprocess_schema(self.tokenizer, table_data)
        if len(table_data['rows']) == 0:
            return
        while (True):
             

            break

