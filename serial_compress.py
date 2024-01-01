import util
from context_window import ContextWindow
from serial_schema import SchemaSerializer
from lsh import BinTable
from code_book import CodeBook

class CompressSerializer(SchemaSerializer):
    def __init__(self):
        super().__init__()
        self.serial_window.set_cell_code_book(CodeBook(self.tokenizer))

    def get_serial_rows(self, table_data, schema_block):
        sorted_bin_lst = schema_block['sorted_bins']
        bin_row_set = set()
        for bin_info in sorted_bin_lst:
            bin_entry = bin_info['bin']
            for row in bin_entry.item_lst:
                if row not in bin_row_set:
                    yield row
                    bin_row_set.add(row)
        
        row_data = table_data['rows']
        row_lst = [a for a, _ in enumerate(row_data)]
        table_row_set = set(row_lst) 
        other_row_set = table_row_set - bin_row_set
        other_row_lst = list(other_row_set)
        for row in other_row_lst:
            yield row

    def get_cell_text(self, cell_info):
        text = cell_info['text']
        if cell_info['size'] < 2:
            return text

        code = self.serial_window.cell_code_book.get_code(cell_info)
        return code

    def preprocess_schema_block(self, table_data, schema_block):
        col_data = table_data['columns']
        bin_entry_lst = []
        for col in schema_block['cols']:
            bin_table = col_data[col]['bin_table'] 
            for bin_entry in bin_table.bin_array:
                if bin_entry is None:
                    continue
                bin_entry_lst.append(bin_entry)
        
        bin_info_lst = []
        for bin_entry in bin_entry_lst: 
            cpr_size = self.compute_bin_cpr_size(table_data, schema_block['cols'], bin_entry)
            bin_info = {'cpr_size':cpr_size, 'bin':bin_entry}
            bin_info_lst.append(bin_info)
        sorted_bin_lst = sorted(bin_info_lst, key=lambda x: x['cpr_size'], reverse=True)
        schema_block['sorted_bins'] = sorted_bin_lst

    def compute_bin_cpr_size(self, table_data, col_lst, bin_entry):
        text_dict = {}
        row_data = table_data['rows']
        for row in bin_entry.item_lst:
            cells = row_data[row]['cells']
            for col in col_lst:
                cell_text = cells[col]['text'] 
                cell_size = cells[col]['size']
                key = util.get_hash_key(cell_text)
                if key not in text_dict:
                    text_dict[key] = {'count':0, 'size':cell_size}
                stat_info = text_dict[key]
                stat_info['count'] += 1
        
        cpr_size = 0
        for key in text_dict:
            stat_info = text_dict[key]
            cpr_size += (stat_info['count'] - 1) * stat_info['size'] 
        return cpr_size

    def hash_row_to_bins(self, table_data):
        col_data = table_data['columns']    
        row_data = table_data['rows']
        num_rows = len(row_data)
        bin_table_lst = []
        for col, col_info in enumerate(col_data):
            infer_type = col_info.get('infer_type', None) 
            if infer_type in [util.CellDataType.FLOAT, util.CellDataType.INT, util.CellDataType.BOOL]:
                continue
            bin_table = BinTable(col, num_rows)
            col_info['bin_table'] = bin_table
            bin_table_lst.append(bin_table)
            for row, row_item in enumerate(row_data):
                cell_info = row_item['cells'][col]
                attr = cell_info['text']
                if attr == '':
                    continue
                attr_key = util.get_hash_key(attr)
                bin_table.add(attr_key, row)
        return bin_table_lst


    def preprocess_other(self, table_data):
        self.hash_row_to_bins(table_data)

