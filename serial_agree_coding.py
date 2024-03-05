import util
from context_window import ContextWindow
from serial import TableSerializer

class AgreeCodingSerializer(TableSerializer):
    def __init__(self):
        super().__init__()
        self.numeric_serializer = None

    def do_serialize(self, table_data):
        return
    
    def compute_agree_set(self, table_data):
        self.create_stripped_partitions(table_data)
        self.compute_row_eq_class(table_data)        
        sefl.compute_maximal_eq_class(table_data)

    def get_table_eq_classes(self, table_data):
        table_eq_classes = []  
        col_data = table_data['columns']
        row_data = table_data['rows']
        for col, col_item in enumerate(col_data):
            eq_class_lst = col_item['eq_class_lst']
            table_eq_classes.extend(eq_class_lst)
        return get_table_eq_classes

    def compute_maximal_eq_class(self, table_data):
        table_eq_classes = self.get_table_eq_classes(table_data)
        N = len(table_eq_classes)
        for i in range(N-1):
            left_class = table_eq_classes[i]
            for j in range(i+1, N):
                right_class = table_eq_classes[j]

            

    def compute_row_eq_class(self, table_data):
        col_data = table_data['columns']
        row_data = table_data['rows']
        for col, col_item in enumerate(col_data):
            eq_class_lst = col_item['eq_class_lst']
            for eq_class in eq_class_lst:
                class_id = eq_class['id']
                row_lst = eq_class['rows']
                for row in row_lst:
                    if 'row_class_set' not in row_item:
                        row_item['row_class_set'] = set() 
                    eq_class_set = row_item['row_class_set'] 
                    eq_class_set.add([col, class_id])

    def create_stripped_partitions(self, table_data):
        col_data = table_data['columns']
        row_data = table_data['rows']
        for col, col_item in enumerate(col_data):
            ptn_dict = {}
            for row, row_item in enumerate(row_data):
                cell_info = row_data[row]['cells'][col]
                cell_text = cell_info['text']
                if cell_text == '':
                    continue
                key = util.get_hash_key(cell_text)
                if key not in ptn_dict:
                    ptn_dict[key] = {'rows':[]}
                row_lst = patn_dict[key]
                row_lst.append(row)
            
            eq_class_lst = []
            class_id = 1
            for key in ptn_dict:
                row_lst = patn_dict[key]
                if len(row_lst) == 1:
                    continue
                eq_class = {
                    'id':class_id,
                    'rows':row_lst
                }
                class_id += 1
                eq_class_lst.append(eq_class)
            col_item['eq_class_lst'] = eq_class_lst
