import util
from context_window import ContextWindow
from serial import TableSerializer

class AttrText:
    def __init__(self, text, size):
        self.text = text
        self.size = size
        self.row_lst = []

    def add_row(self, row):
        self.row_lst.append(row)
    
    @static
    def get_key(text):
        return text.lower()

class ColumnDictionary:
    def __init__(self, col):
        self.col = col
        self.hash_table = dict()
        self.default_attr = AttrText(None, None)
        self.sorted_attrs = []

    def add(self, row, cell):
        text = cell['text']
        if text == '':
            self.default_attr.add_row(row)
            return
        token_size = util.get_token_size(text)
        if token_size == 1:
            self.default_attr.add_row(row)
            return
        key = AttrText.get_key(text)
        if key not in self.hash_table:
            attr = AttrText(text, token_size)
            self.hash_table[key] = attr
        attr_text = self.hash_table[key] 
        sttr_text.add_row(row)

    def process_attrs(self):
        key_lst = []
        for key in self.hash_table:
            attr_text = self.hash_table[key]
            if len(attr_text.row_lst) > 1:
                key_info = {
                    'key':key,
                    'size':attr_text.size
                }
                key_lst.append(key_info)
            else:
                self.default_attr.add_row(attr_text.row_lst[0])

        for to_del_key in del_keys:
            del self.hash_table[to_del_key]

        sorted_key_lst = sorted(key_lst, key=lambda: x['size'], reverse=True)
        for key in sorted_key_lst:
            self.sorted_attrs.append(self.hash_table[key])
        self.sorted_attrs.append(self.default_attr)
        self.hash_table = None 

class CompressSerializer(TableSerializer):
    def __init__(self):
        super().__init__()

    def build_attr_index(self, table_data):
        row_data = table_data['rows']
        col_data = table_data['cols']     
        col_dict_lst = []
        for col, col_info in enumerate(col_data):
            col_dict = ColumnDictionary(col)
            col_dict_lst.append(col_dict)
            for row, row_item in enumerate(row_data):
               cell_lst = row_item['cells']
               cell = cell_lst[col]
               col_dict.add(row, cell)
             
        for col_dict in col_dict_lst:
            col_dict.process_attrs(col_dict)

    def do_serialize(self, table_data):
        self.build_attr_index(table_data)
          


