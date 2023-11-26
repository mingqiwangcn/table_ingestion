import util

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
        
    def serialize(table_data):
        util.preprocess_schema(self.tokenizer, table_data)
        while (True):


            break

