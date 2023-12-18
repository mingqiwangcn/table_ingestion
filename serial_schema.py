import util
from context_window import ContextWindow
from serial import TableSerializer

class SchemaSerializer(TableSerializer):
    def __init__(self):
        super().__init__()

    def get_serial_text(self, table_data, row, col, block_cols):
       cell_info = table_data['rows'][row]['cells'][col]
       col_data = table_data['columns']
       col_info = col_data[col]
       is_last_cell = (col == block_cols[-1])
       sep_token = ';' if not is_last_cell else self.tokenizer.sep_token
       serial_text = cell_info['text'] + ' ' + sep_token + ' ' 
       return serial_text
   
    def do_serialize(self, table_data):
        schema_block_lst = util.split_columns(self.tokenizer, table_data, self.serial_window.wnd_size)
        if len(schema_block_lst) > 1:
            import pdb; pdb.set_trace()
            print('column split')
        for schema_block in schema_block_lst:
            return self.serialize_schema_block(table_data, schema_block) 
  
    def get_schema_text(self, table_data, schema_block):
        schema_text = table_data['documentTitle'] + ' ' + self.tokenizer.sep_token + schema_block['text']
        return schema_text

    def serialize_schema_block(self, table_data, schema_block):
        schema_text = self.get_schema_text(table_data, schema_block)
        self.serial_window.set_schema_text(schema_text)

        row_data = table_data['rows']
        row_cnt = len(row_data)
        block_cols = schema_block['cols']
        for row in range(row_cnt):
            util.preprocess_row(self.tokenizer, row_data[row])
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
            

