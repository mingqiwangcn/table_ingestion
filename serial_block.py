import util
from context_window import ContextWindow
from serial import TableSerializer

class BlockSerializer(TableSerializer):
    def __init__(self):
        super().__init__()

    def get_serial_text(self, table_data, row, col):
       cell_info = table_data['rows'][row_idx]['cells'][col_idx]
       col_data = table_data['columns']
       col_info = col_data[col_idx]
       is_last_cell = (col_idx + 1 == len(col_data))
       sep_token = ';' if not is_last_cell else self.tokenizer.sep_token
       serial_text = col_info['text'] + ' ' + cell_info['text'] + ' ' + sep_token + ' ' 
       return serial_text

    def do_serialize(self, table_data):
        row_data = table_data['rows']
        row_cnt = len(row_data)
        col_cnt = len(table_data['columns'])
        for row in range(row_cnt):
            util.preprocess_row(self.tokenizer, row_data[row])
            for col in range(col_cnt):
                serial_text = self.get_serial_text(table_data, row, col)
                if self.serial_window.can_add(table_data, row, col, serial_text):
                    self.serial_window.add(table_data, row, col)
                else:
                    serial_block = self.serial_window.pop(table_data)
                    yield serial_block
                    self.serial_window.add(table_data, row, col)
        
        if self.serial_window.can_pop():
            serial_block = self.serial_window.pop(table_data)
            yield serial_block

