import util
from context_window import ContextWindow
from bin_packing import bin_pack
from serial import TableSerializer


class SchemaSerializer(TableSerializer):
    def __init__(self):
        super().__init__()

    def get_cell_serial_text(self, col_data, col, cell_info):
        cell_text = cell_info['text']
        cell_serial_text = cell_text + ' ; '
        cell_serial_size = cell_info['size'] + 1
        return cell_serial_text, cell_serial_size

    def get_row_serial_info(self, table_data, row, block_cols):
        col_data = table_data['columns']
        row_cells = table_data['rows'][row]['cells']
        row_serial_cell_lst = []
        for col in block_cols:
            cell_info = row_cells[col] 
            cell_serial_text, cell_serial_size = self.get_cell_serial_text(col_data, col, cell_info)
            cell_info['serial_text'] = cell_serial_text + ' ; '
            cell_info['serial_size'] = cell_serial_size + 1
            row_serial_cell_lst.append(cell_info)
        boundary_cell = row_serial_cell_lst[-1]
        boundary_cell['serial_text'] = boundary_cell['serial_text'].rstrip()[:-1] + ' ' + self.tokenizer.sep_token + ' '
        # row size includes seperator
        row_size = self.serial_window.title_size + sum([a['serial_size'] for a in row_serial_cell_lst])
        row_serial_info = {
            'row':row,
            'cols':block_cols,
            'cell_lst':row_serial_cell_lst,
            'content_size':row_size,
            'use_title':True,
        }
        return row_serial_info
    
    def get_schema_column_text(self, col_name):
        serial_text = col_name + ' ; '  
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
        self.preprocess_other(table_data)
        schema_block_lst = self.split_columns(table_data)
        for schema_block in schema_block_lst:
            yield from self.serialize_schema_block(table_data, schema_block) 
  
    def get_title(self, table_data, schema_block):
        title = table_data['documentTitle'] + ' ; '
        return title

    def preprocess_schema_block(self, table_data, schema_block):
        return

    def serialize_schema_block(self, table_data, schema_block):
        self.preprocess_schema_block(table_data, schema_block)
        title = self.get_title(table_data, schema_block)
        self.serial_window.set_title(title)
        self.serial_window.set_schema(schema_block['text'])

        yield from self.get_wnd_block(table_data, schema_block)

    def get_serial_rows(self, table_data, schema_block):
        row_data = table_data['rows']
        for row, _ in enumerate(row_data):
            yield row
    
    def try_serialize_row(self, table_data, row, block_cols, col_group_lst):
        row_serial_info = self.get_row_serial_info(table_data, row, block_cols)
        f_ok = self.serial_window.can_add(table_data, row, col_group_lst, row_serial_info)
        return f_ok, row_serial_info

    def process_after_not_fit(self, table_data, serial_info):
        return

    def process_after_fit(self, table_data, serial_info):
        return

    def clear_code_book(self):
        return

    def process_before_pop(self):
        return

    def pop_window(self, table_data):
        self.process_before_pop()
        serial_block = self.serial_window.pop(table_data)
        self.clear_code_book()
        return serial_block

    def get_wnd_block(self, table_data, schema_block):
        block_cols = schema_block['cols']
        col_group_lst = [[col]for col in block_cols]
        for row in self.get_serial_rows(table_data, schema_block): 
            fit_ok, serial_info = self.try_serialize_row(table_data, row, block_cols, col_group_lst)
            if fit_ok:
                if serial_info.get('process_add', False):
                    self.process_after_fit(table_data, serial_info)
                self.serial_window.add(table_data, serial_info)
            else:
                if serial_info.get('process_add', False):
                    self.process_after_not_fit(table_data, serial_info)
                serial_block = self.pop_window(table_data)
                yield serial_block
                fit_ok, serial_info = self.try_serialize_row(table_data, row, block_cols, col_group_lst)
                assert(fit_ok)
                if serial_info.get('process_add', False):
                    self.process_after_fit(table_data, serial_info)
                self.serial_window.add(table_data, serial_info)

        if self.serial_window.can_pop():
            serial_block = self.pop_window(table_data)
            yield serial_block
            
