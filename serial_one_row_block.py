from tqdm import tqdm
import util
from context_window import ContextWindow
from serial_block import BlockSerializer

class OneRowBlockSerializer(BlockSerializer):
    def __init__(self):
        super().__init__()

    def do_serialize(self, table_data):
        schema_text = self.get_schema_text(table_data)
        self.serial_window.set_schema_text(schema_text)
        serial_row_col = table_data.get('serial_row_col', None)
        if serial_row_col is None:
            row_data = table_data['rows']
            row_cnt = len(row_data)
            col_cnt = len(table_data['columns'])
            for row in range(row_cnt):
                for col in range(col_cnt):
                    yield from self.serialize_row_col(table_data, row, col)
                if self.serial_window.can_pop():
                    serial_block = self.serial_window.pop(table_data)
                    yield serial_block 
        else:
            for row, col_lst in serial_row_col:
                for col in col_lst:
                    yield from self.serialize_row_col(table_data, row, col)
                if self.serial_window.can_pop():
                    serial_block = self.serial_window.pop(table_data)
                    yield serial_block
