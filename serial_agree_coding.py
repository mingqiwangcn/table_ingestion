import util
from context_window import ContextWindow
from serial import TableSerializer

class AgreeCodingSerializer(TableSerializer):
    def __init__(self):
        super().__init__()
        self.numeric_serializer = None

    def do_serialize(self, table_data):
        agr_dict = self.compute_agree_set(table_data)
        self.make_coding_plan(agr_dict, table_data)

    def compute_agree_set(self, table_data):
        self.create_stripped_partitions(table_data)
        self.compute_row_eq_class(table_data)        
        maximal_class_lst = sefl.compute_maximal_eq_class(table_data)
        row_data = table_data['rows']
        row_num = len(row_data)
        agr_dict = {}
        for maximal_class in maximal_class_lst:
            row_set = maximal_class['row_set']
            for i in range(row_num - 1):
                left_row_class_set = row_data[i]['row_class_set']
                for j in range(row_num):
                    right_row_class_set = row_data[j]['row_class_set']
                    inter_class_set = left_row_class_set.intersection(right_row_class_set)
                    inter_class_lst = list(inter_class_set).sort()
                    key = ','.join(inter_class_lst)
                    if key not in agr_dict:
                        agr_dict[key] = {'agr_class_lst':inter_class_lst, 'agr_row_set':set()}
                    agr_row_set = agr_dict[key]
                    agr_row_set.add(i)
                    agr_row_set.add(j)
        return agr_dict

    def make_coding_plan(self, agr_dict, table_data):
        return

    def get_table_eq_classes(self, table_data):
        table_eq_classes = []  
        col_data = table_data['columns']
        row_data = table_data['rows']
        for col, col_item in enumerate(col_data):
            eq_class_lst = col_item['eq_class_lst']
            table_eq_classes.extend(eq_class_lst)
        return table_eq_classes       

    def compute_maximal_eq_class(self, table_data):
        table_eq_classes = self.get_table_eq_classes(table_data)
        N = len(table_eq_classes)
        for i in range(N):
            left_class = table_eq_classes[i]
            left_set = left_class['row_set']
            for j in range(N):
                if j == i:
                    continue
                right_set = table_eq_classes[j]['row_set']
                if left_set.issubset(righ_set):
                    left_class['maximal'] = False
                    break
    
        maximal_class_lst = []
        for eq_class in table_eq_classes:
           if eq_class.get('maximal', True):
            maximal_class_lst.append(eq_class)
        return maximal_class_lst

    def compute_row_eq_class(self, table_data):
        col_data = table_data['columns']
        row_data = table_data['rows']
        for col, col_item in enumerate(col_data):
            eq_class_lst = col_item['eq_class_lst']
            for eq_class in eq_class_lst:
                class_id = eq_class['id']
                row_lst = list(eq_class['row_set'])
                for row in row_lst:
                    if 'row_class_set' not in row_item:
                        row_item['row_class_set'] = set() 
                    row_class_set = row_item['row_class_set']
                    row_class_info = f'{col}-{class_id}'
                    eq_class_set.add(row_class_info)

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
                    'row_set':set(row_lst)
                }
                class_id += 1
                eq_class_lst.append(eq_class)
            col_item['eq_class_lst'] = eq_class_lst
