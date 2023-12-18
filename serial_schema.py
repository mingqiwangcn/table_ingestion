import util
from context_window import ContextWindow
from serial_block import BlockSerializer

class SchemaSerializer(TableSerializer):
    def __init__(self):
        super().__init__()

    def get_schema_text(self, table_data):
        title = table_data['documentTitle'] + ' ' + self.tokenizer.sep_token


    def get_serial_text(self, table_data, row, col):
       cell_info = table_data['rows'][row_idx]['cells'][col_idx]
       col_data = table_data['columns']
       col_info = col_data[col_idx]
       is_last_cell = (col_idx + 1 == len(col_data))
       sep_token = ';' if not is_last_cell else self.tokenizer.sep_token
       serial_text = col_info['text'] + ' ' + cell_info['text'] + ' ' + sep_token + ' ' 
       return serial_text
