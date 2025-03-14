import util
import transformers
from context_window import ContextWindow

class TableSerializer:
    def __init__(self):
        self.tokenizer = transformers.BertTokenizerFast.from_pretrained('bert-base-uncased')
        wnd_size = util.get_context_window_size(self.tokenizer)
        self.serial_window = self.create_context_window(wnd_size) 

    def create_context_window(self, wnd_size):
        return ContextWindow(self.tokenizer, wnd_size)  

    def get_serial_text(self, table_data, row, col):
        return ''

    def serialize(self, table_data):
        util.preprocess_schema(self.tokenizer, table_data)
        yield from self.do_serialize(table_data)

    def do_serialize(self, table_data):
        return
