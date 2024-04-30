import util
from context_window import ContextWindow
from serial_schema import SchemaSerializer
from lsh import BinTable
from code_book import CodeBook

class CompressSerializer(SchemaSerializer):
    def __init__(self):
        super().__init__()
        self.cell_code_book = CodeBook(self.tokenizer)

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

    def process_before_pop(self):
        special_token_lst = list(self.cell_code_book.special_token_dict.keys())
        self.serial_window.add_special_tokens(special_token_lst)

    def clear_code_book(self):
        self.cell_code_book.reset()
   
    def process_after_not_fit(self, table_data, serial_info):
        cpr_start_cells = serial_info['cpr_start_cells']
        for cell_info in cpr_start_cells:
            code_info = cell_info['code_info']
            pre_cell_lst = code_info['pre_cells']
            for pre_cell in pre_cell_lst:
                del pre_cell['updated_serial_text']
                del pre_cell['updated_serial_size']
            del cell_info['code_info']

    def process_after_fit(self, table_data, serial_info):
        cpr_start_cells = serial_info['cpr_start_cells']
        for cell_info in cpr_start_cells:
            code_info = cell_info['code_info']
            pre_cell_lst = code_info['pre_cells']
            for pre_cell in pre_cell_lst:
                pre_cell['serial_text'] = pre_cell['updated_serial_text']
                pre_cell['serial_size'] = pre_cell['updated_serial_size']   
          
    def calc_row_info(self, table_data, row, block_cols, row_serial_cell_lst):
        cpr_start_cells = [a for a in row_serial_cell_lst if a.get('code_info', None) is not None]

        code_info_lst = []
        pre_cells_to_update = []
        for cell_info in cpr_start_cells:
            code_info = cell_info['code_info']
            code_info_lst.append(code_info)
            pre_cell_lst = code_info['pre_cells']
            pre_cells_to_update.extend(pre_cell_lst)

        code_refer_size = sum([a['code_refer_size'] for a in code_info_lst])
        cell_size = sum([a['serial_size'] for a in row_serial_cell_lst])
        pre_size_chg = sum([(a['updated_serial_size'] - a['serial_size']) for a in  pre_cells_to_update])
        content_size = code_refer_size + cell_size + pre_size_chg
        row_serial_info = {
            'row':row,
            'cols':block_cols,
            'code_refer_size':code_refer_size,
            'cell_size':cell_size,
            'pre_size_chg':pre_size_chg,
            'content_size':content_size,
            'code_info_lst':code_info_lst,
            'cell_lst':row_serial_cell_lst,
            'pre_cells_to_update':pre_cells_to_update,
            'process_add':True,
            'cpr_start_cells':cpr_start_cells
        }
        return row_serial_info

    def update_serial_cell_info(self, row, col_data, col, cell_info):
        cell_text, cell_size = self.get_cell_text(row, col,cell_info)
        cell_info['serial_text'] = cell_text + ' ; '
        cell_info['serial_size'] = cell_size + 1

    def get_row_serial_info(self, table_data, row, block_cols):
        col_data = table_data['columns']
        row_cells = table_data['rows'][row]['cells']
        row_serial_cell_lst = []
        for col in block_cols:
            cell_info = row_cells[col] 
            self.update_serial_cell_info(row, col_data, col, cell_info)
            row_serial_cell_lst.append(cell_info)
            self.update_related_cell(cell_info)
       
        boundary_cell = row_serial_cell_lst[-1]
        boundary_cell['serial_text'] = boundary_cell['serial_text'].rstrip()[:-1] + ' ' + self.tokenizer.sep_token + ' '
        return self.calc_row_info(table_data, row, block_cols, row_serial_cell_lst)
        
    def get_cell_text(self, row, col, cell_info):
        text = cell_info['text']
        if cell_info['size'] < 2:
            return text, cell_info['size']
        return self.cell_code_book.get_code(row, col, cell_info)
        
    def update_related_cell(self, cell_info):
        code_info = cell_info.get('code_info', None)
        if code_info is not None:
            serial_text = cell_info['serial_text']
            serial_size = cell_info['serial_size']
            pre_cell_lst = code_info['pre_cells']
            for pre_cell in pre_cell_lst:
                pre_cell['updated_serial_text'] = serial_text
                pre_cell['updated_serial_size'] = serial_size

    def preprocess_schema_block(self, table_data, schema_block):
        col_data = table_data['columns']
        bin_entry_lst = []
        for col in schema_block['cols']:
            col_info = col_data[col]
            if col_info.get('ignore_row_serial', False):
                continue
            bin_table = col_info['bin_table'] 
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
            if col_info.get('ignore_row_serial', False):
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

