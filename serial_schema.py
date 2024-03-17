import util
from context_window import ContextWindow
from bin_packing import bin_pack
from serial import TableSerializer


class SchemaSerializer(TableSerializer):
    def __init__(self):
        super().__init__()
        self.numeric_serializer = None

    def set_numeric_serializer(self, numeric_serializer):
        self.numeric_serializer = numeric_serializer

    def get_serial_text(self, table_data, row, block_cols):
        col_data = table_data['columns']
        row_cells = table_data['rows'][row]['cells']

        serial_text = ''
        serial_size = 0
        for col in block_cols:
            #if col_info.get('ignore_row_serial', False):
            #    return '', 0
            cell_info = row_cells[col] 
            cell_text = cell_info['text']
            cell_serial_text = cell_text + ' | '
            cell_serial_size = cell_info['size'] + 1
            
            serial_text += cell_serial_text
            serial_size += cell_serial_size

        serial_text = serial_text.rstrip()[:-1] + self.tokenizer.sep_token
        return serial_text, serial_size
 
    def get_schema_column_text(self, col_name):
        serial_text = col_name + ' | '  
        return serial_text

    def split_columns(self, table_data):
        col_data = table_data['columns']
        block_lst = []
        Schema_Max_Size = int(self.serial_window.wnd_size * util.Max_Header_Meta_Ratio)
        
        schema_item_lst = []
        for col, col_info in enumerate(col_data):
            col_name = col_info['text'] 
            serial_text = self.get_schema_column_text(col_name) 
            serial_size = util.get_token_size(self.tokenizer, serial_text)
            schema_item = [col, serial_size, serial_text]
            schema_item_lst.append(schema_item)
        bin_lst = bin_pack(schema_item_lst, Schema_Max_Size) 
        
        for bin_entry in bin_lst:
            block_cols = [a[0] for a in bin_entry.item_lst]
            block_text = ''.join([a[2] for a in bin_entry.item_lst])
            block_size = sum([a[1] for a in bin_entry.item_lst])
            block_info = self.get_block_info(block_cols, block_text, block_size)
            block_lst.append(block_info)
        return block_lst
    
    def get_block_info(self, block_cols, block_text, block_size):
        updated_block_text = block_text.rstrip()[:-1] + self.tokenizer.sep_token
        block_info = {
            'cols':block_cols,
            'text':updated_block_text,
            'size':block_size
        }
        return block_info

    def preprocess_other(self, table_data):
        return

    def do_serialize(self, table_data):
        if self.numeric_serializer is not None:
            self.numeric_serializer.prepare(table_data)
        
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
        yield from self.get_wnd_block(table_data, schema_block)

    def get_wnd_block(self, table_data, schema_block):
        block_cols = schema_block['cols']
        for row in self.get_serial_rows(table_data, schema_block): 
            serial_text, serial_size = self.get_serial_text(table_data, row, block_cols)
            col_group_lst = [[col]for col in block_cols]
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
            
