from serial_compress import CompressSerializer
from schema_code_book import SchemaCodeBook

class SchemaCellCodingSerializer(CompressSerializer):
    def __init__(self):
        super().__init__()
        self.schema_code_book = SchemaCodeBook(self.tokenizer)

    def get_title(self, table_data, schema_block):
        title = table_data['documentTitle'] + ' ' + self.tokenizer.sep_token + ' '
        return title

    def process_before_pop(self):
        super().process_before_pop()
        special_token_lst = list(self.schema_code_book.special_token_dict.keys())
        self.serial_window.add_special_tokens(special_token_lst)

    def clear_code_book(self):
        super().clear_code_book()
        self.schema_code_book.reset()

    def process_after_not_fit(self, table_data, serial_info):
        super().process_after_not_fit(table_data, serial_info)
        cpr_start_cells = serial_info['schema_cpr_start_cells']
        for cell_info in cpr_start_cells:
            code_info = cell_info['schema_code_info']
            pre_cell_lst = code_info['pre_cells']
            for pre_cell in pre_cell_lst:
                del pre_cell['updated_schema']
            del cell_info['schema_code_info']

    def process_after_fit(self, table_data, serial_info):
        super().process_after_fit(table_data, serial_info)
        cpr_start_cells = serial_info['schema_cpr_start_cells']
        for cell_info in cpr_start_cells:
            code_info = cell_info['schema_code_info']
            pre_cell_lst = code_info['pre_cells']
            for pre_cell in pre_cell_lst:
                pre_cell['schema'] = pre_cell['updated_schema']
        
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

        schema_size = sum([a['schema'][1] + 1 for a in row_serial_cell_lst])
        code_refer_size = sum([a['code_refer_size'] for a in code_info_lst])
        pre_size_chg = sum([(a['updated_schema'][1] - a['schema'][1]) for a in  pre_cells_to_update])
        row_serial_info['content_size'] += schema_size + code_refer_size + pre_size_chg
        row_serial_info['schema_code_info_lst'] = code_info_lst
        row_serial_info['schema_pre_cells_to_update'] = pre_cells_to_update
        row_serial_info['schema_cpr_start_cells'] = cpr_start_cells
        return row_serial_info

    def get_schema_info(self, col_data, col, cell_info):
        column_info = col_data[col]
        text = column_info['text']
        size = column_info['size']
        if size < 2:
            return text, size
        code, code_size = self.schema_code_book.get_code(col_data, col, cell_info)
        return code, code_size

    def update_related_cell(self, cell_info):
        super().update_related_cell(cell_info)
        schema_code_info = cell_info.get('schema_code_info', None)
        if schema_code_info is not None:
            schema_info = cell_info['schema']
            pre_cell_lst = schema_code_info['pre_cells']
            for pre_cell in pre_cell_lst:
                pre_cell['updated_schema'] = schema_info

    def update_serial_cell_info(self, row, col_data, col, cell_info):
        super().update_serial_cell_info(row, col_data, col, cell_info)
        schema_code, schema_size = self.get_schema_info(col_data, col, cell_info)
        cell_info['schema'] = (schema_code, schema_size) 
    