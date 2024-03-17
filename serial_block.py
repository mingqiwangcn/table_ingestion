from tqdm import tqdm
import util
from context_window import ContextWindow
from serial import TableSerializer

class BlockSerializer(TableSerializer):
    def __init__(self):
        super().__init__()

    def get_serial_text(self, table_data, row, col):
       cell_info = table_data['rows'][row]['cells'][col]
       col_data = table_data['columns']
       col_info = col_data[col]
       is_last_cell = (col + 1 == len(col_data))
       sep_token = ';' if not is_last_cell else self.tokenizer.sep_token
       serial_text = col_info['text'] + ' : ' + cell_info['text'] + ' ' + sep_token + ' '
       serial_size = col_info['size'] + 1 + cell_info['size'] + 1
       return serial_text, serial_size

    def get_schema_text(self, table_data):
        title = table_data['documentTitle'] + ' ' + self.tokenizer.sep_token
        return title 

    def do_serialize(self, table_data):
        schema_text = self.get_schema_text(table_data)
        self.serial_window.set_schema_text(schema_text)

        row_data = table_data['rows']
        row_cnt = len(row_data)
        col_cnt = len(table_data['columns'])
        for row in range(row_cnt):
            for col in range(col_cnt):
                serial_text, serial_size = self.get_serial_text(table_data, row, col)
                col_group_lst = [[col]]
                fit_ok, serial_info = self.serial_window.can_add(table_data, row, col_group_lst, serial_text, serial_size)
                if fit_ok:
                    self.serial_window.add(table_data, serial_info)
                else:
                    serial_block = self.serial_window.pop(table_data)
                    yield serial_block
                    self.serial_window.add(table_data, serial_info)
        
        if self.serial_window.can_pop():
            serial_block = self.serial_window.pop(table_data)
            yield serial_block

