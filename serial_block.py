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

        title_text = ''
        title_size = 0
        if col == 0 or len(self.serial_window.content_buffer) == 0:
            title_text = self.serial_window.title
            title_size = self.serial_window.title_size

        cell_info['serial_text'] = title_text + col_info['text'] + ' : ' + cell_info['text'] + ' ' + sep_token + ' '
        cell_info['serial_size'] = title_size + col_info['size'] + 1 + cell_info['size'] + 1
        
        serial_cell_lst = [cell_info]
        content_size = cell_info['serial_size']
        serial_info = {
            'row':row,
            'cols':[col],
            'cell_lst':serial_cell_lst,
            'content_size':content_size
        }

        return serial_info

    def get_title(self, table_data):
        title = table_data['documentTitle'] + ' ' + self.tokenizer.sep_token
        return title 


    def serialize_row_col(self, table_data, row, col):
        serial_info = self.get_serial_text(table_data, row, col)
        col_group_lst = [[col]]
        fit_ok = self.serial_window.can_add(table_data, row, col_group_lst, serial_info)
        if fit_ok:
            self.serial_window.add(table_data, serial_info)
        else:
            serial_block = self.serial_window.pop(table_data)
            yield serial_block
            self.serial_window.add(table_data, serial_info)

    def do_serialize(self, table_data):
        schema_text = self.get_title(table_data)
        self.serial_window.set_title(schema_text)
        serial_row_col = table_data.get('serial_row_col', None)
        if serial_row_col is None:
            row_data = table_data['rows']
            row_cnt = len(row_data)
            col_cnt = len(table_data['columns'])
            for row in range(row_cnt):
                for col in range(col_cnt):
                    yield from self.serialize_row_col(table_data, row, col) 
        else:
            for row, col_lst in serial_row_col:
                for col in col_lst:
                    yield from self.serialize_row_col(table_data, row, col)

        if self.serial_window.can_pop():
            serial_block = self.serial_window.pop(table_data)
            yield serial_block

