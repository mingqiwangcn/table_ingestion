import util
from context_window import ContextWindow
from serial_compress import CompressSerializer
from serial_schema import SchemaSerializer
from context_window import ContextWindow

class CprScmContextWindow(ContextWindow):
    def __init__(self, tokenizer, wnd_size):
        super().__init__(tokenizer, wnd_size)

    def can_add(self, table_data, row_idx, col_idx, serial_text):
        cell_info = table_data['rows'][row_idx]['cells'][col_idx]
        col_data = table_data['columns']
        col_info = col_data[col_idx]

        scm_serial_text = cell_info['scm_serial_text']
        scm_token_size = util.get_token_size(self.tokenizer, scm_serial_text)
        cell_info['scm_serial_size'] = scm_token_size
        cell_info['serial_text'] = serial_text
        cell_info['serial_size'] = None
        
        cell_info['row'] = row_idx
        cell_info['col'] = col_idx
       
        code_size = 0
        updated_buffer_size = self.buffer_size
        compress_code = cell_info.get('compress_code', None)
        if self.schema_size + code_size + updated_buffer_size + scm_token_size > self.wnd_size:
            if compress_code is not None:
                del cell_info['compress_code']
                pre_cell_lst = cell_info['pre_cells']
                for pre_cell in pre_cell_lst:
                    del pre_cell['updated_serial_text']
                    del pre_cell['updated_serial_size']
                del cell_info['pre_cells']
            return False
        return True
   
    def add(self, table_data, row_idx, col_idx):
        cell_info = table_data['rows'][row_idx]['cells'][col_idx]
        scm_token_size = cell_info['scm_serial_size']
        self.buffer_size += scm_token_size
        self.text_buffer.append(cell_info)
        compress_code = cell_info.get('compress_code', None)
        if compress_code is not None:
            pre_cell_lst = cell_info['pre_cells']
            #first_cell_size_chg = first_cell['updated_serial_size'] - first_cell['serial_size']                
            #self.buffer_size += first_cell_size_chg
            for pre_cell in pre_cell_lst:
                pre_cell['serial_text'] = pre_cell['updated_serial_text']
            #first_cell['serial_size'] = first_cell['updated_serial_size']
            self.cell_code_book.code_text += compress_code
            #self.cell_code_book.code_size += cell_info['cpr_code_size']

    def pop(self, table_data):
        assert(len(self.text_buffer) > 0)
        text_lst = [a['serial_text'] for a in self.text_buffer]
        code_text = ''
        code_size = 0
        special_token_lst = None
        if self.cell_code_book is not None:
            code_text = self.cell_code_book.code_text
            code_size = self.cell_code_book.code_size
            special_token_lst = list(self.cell_code_book.special_token_dict.keys())

        out_text = self.schema_text + code_text + ''.join(text_lst)
        
        scm_text_lst = [a['scm_serial_text'] for a in self.text_buffer]
        
        out_scm_text = self.schema_text + ''.join(scm_text_lst)
        out_scm_size = self.schema_size + self.buffer_size
        
        out_data = {
            'passage':out_text,
            'scm_passage':out_scm_text,
            'tag':{
                'size':out_scm_size,
                'table_id':table_data['tableId'],
                'row':[a['row'] for a in self.text_buffer],
                'col':[a['col'] for a in self.text_buffer],
                'special_tokens':special_token_lst,
            }
        }
        
        assert(len(self.tokenizer.tokenize(out_text)) <= self.wnd_size)
        assert(len(self.tokenizer.tokenize(out_scm_text)) <= self.wnd_size)
        
        self.reset()
        return out_data


class CprScmSerializer(CompressSerializer):
    def __init__(self):
        super().__init__()
    
    def create_context_window(self, wnd_size):
        return CprScmContextWindow(self.tokenizer, wnd_size)

    def get_wnd_block(self, table_data, schema_block):
        row_data = table_data['rows']
        block_cols = schema_block['cols']
        row_daat = table_data['rows']
        for row in self.get_serial_rows(table_data, schema_block): 
            for col in block_cols:
                serial_scm_text = SchemaSerializer.get_serial_text(self, table_data, row, col, block_cols) 
                serial_cpr_text = self.get_serial_text(table_data, row, col, block_cols)
                cell_info = row_data[row]['cells'][col]
                cell_info['scm_serial_text'] = serial_scm_text
                if self.serial_window.can_add(table_data, row, col, serial_cpr_text):
                    self.serial_window.add(table_data, row, col)
                else:
                    serial_block = self.serial_window.pop(table_data)
                    yield serial_block
                    self.serial_window.add(table_data, row, col)
        
        if self.serial_window.can_pop():
            serial_block = self.serial_window.pop(table_data)
            yield serial_block 

