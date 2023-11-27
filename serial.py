import util
from context_window import ContextWindow

class TableSerializer:
    def __init__(self):
        self.tokenizer = transformers.BertTokenizerFast.from_pretrained('bert-base-uncased')
        wnd_size = util.get_context_window_size(self.tokenizer)
        self.serial_window = ContextWindow(self.tokenizer, wnd_size) 

    def serialize(table_data):
        util.preprocess_schema(self.tokenizer, table_data)
        col_data = table_data['cols']
        row_data = table_data['rows']
        for row_item in row_data:
            cell_lst = row_item['cells']
            for col_idx, cell_info in enumerate(cell_lst):
                col_info = col_data[col_idx]

                

