import util

class NumericSerializer:
    def __init__(self):
        return

    def prepare(self, table_data):
        util.infer_col_type(table_data)
        col_data = table_data['columns']
        ignore_type_lst = [util.CellDataType.FLOAT, util.CellDataType.INT, 
                           util.CellDataType.BOOL, util.CellDataType.POLYGON]
        for col, col_info in enumerate(col_data):
            infer_type = col_info.get('infer_type', None)
            if infer_type in ignore_type_lst:
                col_info['ignore_row_serial'] = True

