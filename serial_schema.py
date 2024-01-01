import util
from context_window import ContextWindow
from serial import TableSerializer

class SchemaSerializer(TableSerializer):
    def __init__(self):
        super().__init__()

    def get_cell_text(self, cell_info):
        return cell_info['text']

    def get_serial_text(self, table_data, row, col, block_cols):
        cell_info = table_data['rows'][row]['cells'][col]
        col_data = table_data['columns']
        col_info = col_data[col]
        is_last_cell = (col == block_cols[-1])
        sep_token = ';' if not is_last_cell else self.tokenizer.sep_token
       
        cell_text = self.get_cell_text(cell_info)
        serial_text = cell_text + ' ' + sep_token + ' '
        compress_code = cell_info.get('compress_code', None) 
        if compress_code is not None:
            first_cell = cell_info['first_cell']
            first_cell['updated_serial_text'] = serial_text
            first_cell['updated_serial_size'] = util.get_token_size(self.tokenizer, first_cell['updated_serial_text'])

        return serial_text
 
    def get_schema_column_text(self, col_name):
        serial_text = col_name + ' ;'  
        return serial_text

    def split_columns(self, table_data):
        col_data = table_data['columns']
        block_lst = []
        block_cols = []
        block_text = ''
        block_size = 0
        max_size = int(self.serial_window.wnd_size * util.Max_Header_Meta_Ratio)
        for offset, col_info in enumerate(col_data):
            col_name = col_info['text'] 
            serial_text = self.get_schema_column_text(col_name) 
            serial_size = util.get_token_size(self.tokenizer, serial_text)
            if block_size + serial_size <= max_size:
                block_cols.append(offset)
                block_text += ' ' + serial_text
                block_size += serial_size
            else:
                block_info = self.get_block_info(block_cols, block_text, block_size)
                block_lst.append(block_info)
                block_cols = [offset]
                block_text = serial_text
                block_size = serial_size

        if block_size > 0:
            block_info = self.get_block_info(block_cols, block_text, block_size)
            block_lst.append(block_info)
        return block_lst
    
    def get_block_info(self, block_cols, block_text, block_size):
        assert block_text[-1] == ';'
        updated_block_text = block_text[:-1] + self.tokenizer.sep_token
        block_info = {
            'cols':block_cols,
            'text':updated_block_text,
            'size':block_size
        }
        return block_info

    def preprocess_row_data(self, table_data):
        row_data = table_data['rows']
        for row_item in row_data:
            util.preprocess_row(self.tokenizer, row_item)

    def preprocess_other(self, table_data):
        return

    def do_serialize(self, table_data):
        self.preprocess_row_data(table_data)
        self.preprocess_other(table_data)
        schema_block_lst = self.split_columns(table_data)
        for schema_block in schema_block_lst:
            yield from self.serialize_schema_block(table_data, schema_block) 
  
    def get_window_schema_text(self, table_data, schema_block):
        schema_text = table_data['documentTitle'] + ' ' + self.tokenizer.sep_token + schema_block['text']
        return schema_text

    def preprocess_schema_block(self, table_data, schema_block):
        return

    def get_serial_rows(self, table_data, schema_block):
        row_data = table_data['rows']
        for row, _ in enumerate(row_data):
            yield row

    def serialize_schema_block(self, table_data, schema_block):
        self.preprocess_schema_block(table_data, schema_block)

        schema_text = self.get_window_schema_text(table_data, schema_block)
        self.serial_window.set_schema_text(schema_text)

        block_cols = schema_block['cols']
        for row in self.get_serial_rows(table_data, schema_block): 
            for col in block_cols:
                serial_text = self.get_serial_text(table_data, row, col, block_cols)
                if self.serial_window.can_add(table_data, row, col, serial_text):
                    self.serial_window.add(table_data, row, col)
                else:
                    serial_block = self.serial_window.pop(table_data)
                    yield serial_block
                    self.serial_window.add(table_data, row, col)
        
        if self.serial_window.can_pop():
            serial_block = self.serial_window.pop(table_data)
            yield serial_block
            

