import util
from context_window import ContextWindow
from serial import TableSerializer

class NumOptSerializer(TableSerializer):
    def __init__(self):
        super().__init__()
   
    def init_special(self):
        special_tokens = ['[PR_KEY]']
        for col in range(util.MAX_WND_COLS):
            token = self.get_col_idx_token(col + 1)
            special_tokens.append(token)

        self.tokenizer.add_special_tokens(special_tokens)

    def get_col_idx_token(self, col):
        token = '[COL_%d]' % col
        return token

    def get_serial_text(self, table_data, row, col):
        cell_info = table_data['rows'][row_idx]['cells'][col_idx]
        col_data = table_data['columns']
        col_info = col_data[col_idx]
        is_last_cell = (col_idx + 1 == len(col_data))
        sep_token = ';' if not is_last_cell else self.tokenizer.sep_token
        serial_text = col_info['text'] + ' ' + cell_info['text'] + ' ' + sep_token + ' ' 
        return serial_text

    def get_header_serial_text(self, header_text, offset):
        serial_text = self.get_col_idx_token(offset) + ' is ' + header_text  
        return serial_text

    def can_header_meta_fit(self, table_data):
        col_data = table_data['columns']
        if len(col_data) > util.MAX_WND_COLS:
            return False
        serial_text_lst = []
        for col, col_info in enumerate(col_data):
            offset = col
            header_text = col_info['text']
            serial_text = self.get_header_serial_text(header_text, offset)
            serial_text_lst.append(serial_text)

        meta_text = ' ; '.join(serial_text_lst) + ' ' + self.tokenizer.sep_token
        meta_size = util.get_token_size(meta_text)
        max_meta_size = int(self.serial_window.wnd_size * util.Max_Header_Meta_Ratio)
        is_fit = (meta_size <= max_meta_size)
         

    def can_numeric_optmization(self, col_data):
        for col_info in col_data:
            infer_type = col_info.get('infer_type', None)
            if infer_type in [CellDataType.FLOAT, CellDataType.BOOL]:
                return True
        return False

    def simple_serialize(self, table_data):
        row_data = table_data['rows']
        row_cnt = len(row_data)
        col_cnt = len(table_data['columns'])
        for row in range(row_cnt):
            for col in range(col_cnt):
                if (self.serial_window.can_add(table_data, row, col)):
                    self.serial_window.add(table_data, row, col)
                else:
                    serial_block = self.serial_window.pop(table_data)
                    yield serial_block
                    self.serial_window.add(table_data, row, col)
        
        if self.serial_window.can_pop():
            serial_block = self.serial_window.pop(table_data)
            yield serial_block


    def do_serialize(self, table_data):
        self.infer_col_type(table_data)
        col_data = table_data['columns']
        use_num_opt = self.can_numeric_optmization(col_data)
        header_meta_fit = self.can_header_meta_fit(col_data)
        if (not use_num_opt) and header_meta_fit:
            #No need to split columns. 
        else:
            #Need to split columns and use [PR_KEY] column


