import util
from context_window import ContextWindow
from serial import TableSerializer

class BlockSerializer(TableSerializer):
    def __init__(self):
        super().__init__()

    def do_serialize(self, table_data):
        row_data = table_data['rows']
        row_cnt = len(row_data)
        col_cnt = len(table_data['columns'])
        for row in range(row_cnt):
            util.preprocess_row(self.tokenizer, row_data[row])
            for col in range(col_cnt):
                if (self.serial_window.can_add(table_data, row, col)):
                    self.serial_window.add(table_data, row, col)
                else:
                    serial_block = self.serial_window.pop(table_data)
                    yield serial_block
                    self.serial_window.add(table_data, row, col)
        
        if self.serial_window.can_pop():
            serial_block = self.serial_window.pop(table_data)
            yield serial_block

