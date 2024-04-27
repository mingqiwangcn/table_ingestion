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
    
    def update_serial_cell_info(self, col_data, col, cell_info):
        super().update_serial_cell_info(col_data, col, cell_info)
        schema_code, schema_size = self.get_schema_info(col_data, col)
        cell_info['schema'] = (schema_code, schema_size) 

    def process_before_add(self, table_data, serial_info):
        super().process_before_add(table_data, serial_info)
