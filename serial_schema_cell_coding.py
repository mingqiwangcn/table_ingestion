from serial_compress import CompressSerializer
from schema_code_book import SchemaCodeBook
from context_window_coding import ContextWindowCoding

class SchemaCellCodingSerializer(CompressSerializer):
    def __init__(self):
        super().__init__()
        self.schema_code_book = SchemaCodeBook(self.tokenizer)

    def get_schema_info(self, col_data, col):
        column_info = col_data[col]
        text = column_info['text']
        size = column_info['size']
        if size < 2:
            return text, size
        # Use more efficient way 
        code, code_size = self.schema_code_book.get_code(col_data, col)
        return code, code_size

    def update_related_cell(self, cell_info):
        super().update_related_cell(cell_info)
        schema_code_info = cell_info.get('schema_code_info', None)
        if schema_code_info is not None:
            schema_info = cell_info['schema']
            pre_cell_lst = code_info['pre_cells']
            for pre_cell in pre_cell_lst:
                pre_cell['updated_schema'] = schema_info

    def update_serial_cell_info(self, col_data, col, cell_info):
        super().update_serial_cell_info(col_data, col, cell_info)
        schema_code, schema_size = self.get_schema_info(col_data, col)
        cell_info['schema'] = (schema_code, schema_size) 

    def process_after_fit(self, table_data, serial_info):
        super().process_before_add(table_data, serial_info)
        special_token_lst = list(self.schema_code_book.special_token_dict.keys())
        self.serial_window.add_special_tokens(special_token_lst)
        cpr_start_cells = serial_info['schema_cpr_start_cells']
        for cell_info in cpr_start_cells:
            code_info = cell_info['schema_code_info']
            pre_cell_lst = code_info['pre_cells']
            for pre_cell in pre_cell_lst:
                pre_cell['schema'] = pre_cell['updated_schema']

        cell_lst = serial_info['cell_lst']
        for cell_info in cell_lst:
            cell_info['serial_text'] = cell_info['schema'][0] + ' : ' + cell_info['serial_text']
            cell_info['serial_size'] = cell_info['schema'][1] + 1 + cell_info['serial_size']
        
    def calc_row_info(self, table_data, row, block_cols, row_serial_cell_lst):
        row_serial_info = super().calc_row_info(table_data, row, block_cols, row_serial_cell_lst)
        cpr_start_cells = [a for a in row_serial_cell_lst if a.get('schema_code_info', None) is not None]

        code_info_lst = []
        pre_cells_to_update = []
        for cell_info in cpr_start_cells:
            code_info = cell_info['schema_code_info']
            code_info_lst.append(code_info)
            pre_cell_lst = code_info['pre_cells']
            pre_cells_to_update.extend(pre_cell_lst)

         row_serial_info['schema_code_info_lst'] = code_info_lst
         row_serial_info['schema_pre_cells_to_update'] = pre_cells_to_update
         row_serial_info['schema_cpr_start_cells'] = cpr_start_cells

    def clear_code_book(self):
        super().clear_code_book()
        self.schema_code_book.reset()