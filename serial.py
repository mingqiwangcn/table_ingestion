import util
import transformers

class TableSerializer:
    def __init__(self):
        self.tokenizer = transformers.BertTokenizerFast.from_pretrained('bert-base-uncased')
        wnd_size = util.get_context_window_size(self.tokenizer)
        self.serial_window = ContextWindow(self.tokenizer, wnd_size) 

    def serialize(self, table_data):
        util.preprocess_schema(self.tokenizer, table_data)
        self.do_serialize(table_data)

    def do_serialize(self, table_data):
        return
