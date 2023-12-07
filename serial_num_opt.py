import util
from context_window import ContextWindow
from serial import TableSerializer

class CellDataType:
    INT = 1
    BOOL = 2
    FLOAT = 3
     

class NumOptSerializer(TableSerializer):
    def __init__(self):
        super().__init__()

    def infer_col_type(self, table_data):
        col_data = table_data['columns']
        row_data = table_data['rows']
        for col, col_info in enumerate(col_data):
            type_lst = []
            for row_item in row_data:
                util.preprocess_row(self.tokenizer, row_data[row])
                cell_info = row_item['cells'][col]
                cell_text = cell_info['text']
                infer_type = None
                if (cell_text != ''):
                    if util.is_bool(cell_text):
                        infer_type = CellDataType.BOOL
                    elif util.is_float(cell_text):
                        if util.is_int(cell_text):
                            infer_type = CellDataType.INT
                        else:
                            infer_type = CellDataType.FLOAT
                    else:
                        infer_type = None
                    if infer_type is not None:
                        cell_info['infer_type'] = infer_type
                        type_lst.append(infer_type)
            
            if len(type_lst) > 0:
                if all(type_lst == CellDataType.BOOL):
                    col_info['infer_type'] = CellDataType.BOOL
                elif all(type_lst == CellDataType.INT):
                    col_info['infer_type'] = CellDataType.INT
                elif all([a in (CellDataType.FLOAT, CellDataType.INT) for a in type_lst])
                    col_info['infer_type'] = CellDataType.FLOAT
        

    def get_serial_text(self, table_data, row, col):
        cell_info = table_data['rows'][row_idx]['cells'][col_idx]
        col_data = table_data['columns']
        col_info = col_data[col_idx]
        is_last_cell = (col_idx + 1 == len(col_data))
        sep_token = ';' if not is_last_cell else self.tokenizer.sep_token
        serial_text = col_info['text'] + ' ' + cell_info['text'] + ' ' + sep_token + ' ' 
        return serial_text

    def do_serialize(self, table_data):
        self.infer_col_type(table_data)
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

