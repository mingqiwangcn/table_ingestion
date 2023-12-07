import util
from context_window import ContextWindow
from serial import TableSerializer

class AttrText:
    def __init__(self, col, text, size):
        self.col = col
        self.text = text
        self.size = size
        self.row_lst = []

    def add_row(self, row):
        self.row_lst.append(row)
    
    @staticmethod
    def get_key(text):
        return text.lower()

class ColumnDictionary:
    def __init__(self, tokenizer, col):
        self.tokenizer = tokenizer
        self.col = col
        self.hash_table = dict()
        self.default_attr = AttrText(self.col, None, 0)

    def add(self, row, cell):
        text = cell['text']
        if text == '':
            self.default_attr.add_row(row)
            return
        token_size = util.get_token_size(self.tokenizer, text)
        if token_size == 1:
            self.default_attr.add_row(row)
            return
        key = AttrText.get_key(text)
        if key not in self.hash_table:
            attr = AttrText(self.col, text, token_size)
            self.hash_table[key] = attr
        attr_text = self.hash_table[key] 
        attr_text.add_row(row)

    def get_attrs(self):
        key_lst = []
        for key in self.hash_table:
            attr_text = self.hash_table[key]
            if len(attr_text.row_lst) > 1:
                key_lst.append(key)
            else:
                self.default_attr.add_row(attr_text.row_lst[0])

        col_attr_lst = []
        for key in key_lst:
            col_attr_lst.append(self.hash_table[key])
        col_attr_lst.append(self.default_attr)
        self.hash_table = None

        return col_attr_lst

class CompressSerializer(TableSerializer):
    def __init__(self):
        super().__init__()

    def build_attr_index(self, table_data):
        row_data = table_data['rows']
        col_data = table_data['columns']     
        col_dict_lst = []
        for col, col_info in enumerate(col_data):
            col_dict = ColumnDictionary(self.tokenizer, col)
            col_dict_lst.append(col_dict)
            for row, row_item in enumerate(row_data):
               cell_lst = row_item['cells']
               cell = cell_lst[col]
               col_dict.add(row, cell)
    
        attr_lst = []
        for col_dict in col_dict_lst:
            col_attr_lst = col_dict.get_attrs()
            attr_lst.extend(col_attr_lst)
        
        sorted_attr_lst = sorted(attr_lst, key=lambda x: x.size, reverse=True)
        return sorted_attr_lst

    def do_serialize(self, table_data):
        #if table_data['tableId'] == 'h6r6-h5c9':
        #    import pdb; pdb.set_trace()
        sorted_attr_lst = self.build_attr_index(table_data)
        done_row_dict = dict()
        for attr_text in sorted_attr_lst:
            

