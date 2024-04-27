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

    def process_before_add(self, table_data, serial_info):
        super().process_before_add(table_data, serial_info)
        cpr_start_cells = serial_info['schema_cpr_start_cells']
        for cell_info in cpr_start_cells:
            code_info = cell_info['schema_code_info']
            pre_cell_lst = code_info['pre_cells']
            for pre_cell in pre_cell_lst:
                pre_cell['schema'] = pre_cell['updated_schema']
                
    def calc_row_size(self, table_data, row, block_cols, row_serial_cell_lst):
        row_serial_info = super().calc_row_size(table_data, row, block_cols, row_serial_cell_lst)
        cpr_start_cells = [a for a in row_serial_cell_lst if a.get('schema_code_info', None) is not None]
        pre_size_chg = 0
        code_refer_lst = []
        code_refer_size = 0
        for cell_info in cpr_start_cells:
            code_info = cell_info['schema_code_info']
            code_refer_lst.append(code_info['code_refer'])    
            code_refer_size += code_info['code_refer_size']
            pre_cell_lst = code_info['pre_cells']
            for pre_cell in pre_cell_lst:
                pre_cell_size_chg = pre_cell['updated_schema'][1] - pre_cell['schema'][1]       
                pre_size_chg += pre_cell_size_chg
         
         row_serial_info['code_refer_lst'] = code_refer_lst + row_serial_info['code_refer_lst']
         row_serial_info['content_size'] += pre_size_chg
         row_serial_info['schema_cpr_start_cells'] = cpr_start_cells
         