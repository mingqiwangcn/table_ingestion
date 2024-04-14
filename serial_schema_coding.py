from serial_schema import SchemaSerializer
from schema_code_book import SchemaCodeBook

class SchemaCodingSerializer(SchemaSerializer):
    def __init__(self):
        super().__init__()
        code_book = SchemaCodeBook(self.tokenizer)
        self.serial_window.set_cell_code_book(code_book)

    def get_window_schema_text(self, table_data, schema_block):
        schema_text = table_data['documentTitle'] + ' ' + self.tokenizer.sep_token + ' '
        return schema_text

    def get_wnd_block(self, table_data, schema_block):
        col_data = table_data['columns']
        row_data = table_data['rows']
        row_cnt = len(row_data)
        block_cols = schema_block['cols']
        for row in range(row_cnt):
            row_cells = row_data[row]['cells']
            for col in block_cols:
                cell_info = row_cells[col]
                is_last_cell = (col == block_cols[-1])
                sep_token = ';' if not is_last_cell else self.tokenizer.sep_token
                serial_cell_lst = self.get_serial_text(col_data, col, cell_info, sep_token)
                col_group_lst = [[col]]
                fit_ok, serial_info = self.serial_window.can_add(table_data, row, col_group_lst, serial_cell_lst)
                if fit_ok:
                    self.serial_window.add(table_data, serial_info)
                else:
                    serial_block = self.serial_window.pop(table_data)
                    yield serial_block
                    self.serial_window.add(table_data, serial_info)
        
        if self.serial_window.can_pop():
            serial_block = self.serial_window.pop(table_data)
            yield serial_block

    def get_serial_text(self, col_data, col, cell_info, sep_token):
        column_info = col_data[col]
        if column_info['size'] < 2:
            schema_code = column_info['text']
            schem_code_size = column_info['size']
        else:
            code_book = self.serial_window.cell_code_book
            schema_code, schem_code_size = code_book.get_code(col_data, col, cell_info)
        cell_info['serial_text'] = schema_code + ' : ' + cell_info['text'] + sep_token + '  '
        cell_info['serial_size'] = schem_code_size + 1 + cell_info['size'] + 1
        self.update_related_cell(cell_info)
        return [cell_info]
    
    def update_related_cell(self, cell_info):
        compress_code = cell_info.get('compress_code', None) 
        if compress_code is not None:
            serial_text = cell_info['serial_text']
            serial_size = cell_info['serial_size']
            pre_cell_lst = cell_info['pre_cells']
            for pre_cell in pre_cell_lst:
                pre_cell['updated_serial_text'] = serial_text
                pre_cell['updated_serial_size'] =serial_size