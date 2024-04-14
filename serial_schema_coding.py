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

    def get_cell_serial_text(self, col_data, col, cell_info):
        column_info = col_data[col]
        if column_info['size'] < 2:
            schema_code = column_info['text']
            schem_code_size = column_info['size']
        else:
            code_book = self.serial_window.cell_code_book
            schema_code, schem_code_size = code_book.get_code(col_data, col, cell_info)
        cell_serial_text = schema_code + ' : ' + cell_info['text'] + ' ; '
        cell_serial_size = schem_code_size + 1 + cell_info['size'] + 1
        return cell_serial_text, cell_serial_size
    
    def get_serial_text(self, table_data, row, block_cols):
        col_data = table_data['columns']
        row_cells = table_data['rows'][row]['cells']
        row_serial_cell_lst = []
        for col in block_cols:
            cell_info = row_cells[col] 
            cell_serial_text, cell_serial_size = self.get_cell_serial_text(col_data, col, cell_info)
            cell_info['serial_text'] = cell_serial_text
            cell_info['serial_size'] = cell_serial_size
            row_serial_cell_lst.append(cell_info)
            self.update_related_cell(cell_info, row)

        boundary_cell = row_serial_cell_lst[-1]
        boundary_cell['serial_text'] = boundary_cell['serial_text'].rstrip()[:-1] + ' ' + self.tokenizer.sep_token + ' '
        return row_serial_cell_lst

    def update_related_cell(self, cell_info, row):
        compress_code = cell_info.get('compress_code', None) 
        if compress_code is not None:
            serial_text = cell_info['serial_text']
            serial_size = cell_info['serial_size']
            pre_cell_lst = cell_info['pre_cells']
            for pre_cell in pre_cell_lst:
                pre_cell['updated_serial_text'] = serial_text
                pre_cell['updated_serial_size'] =serial_size