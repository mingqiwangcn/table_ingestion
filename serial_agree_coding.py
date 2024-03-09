from tqdm import tqdm
import util
from context_window import ContextWindow
from serial import TableSerializer
import time
import torch
import heapq

class AgreeCodingSerializer(TableSerializer):
    def __init__(self):
        super().__init__()
        self.numeric_serializer = None

    def do_serialize(self, table_data):
        print('start compute agr set')
        agr_dict = self.compute_agree_set(table_data)
        pq_agr_key_lst = self.create_agr_priority_queue(agr_dict)
        print('agr set ok')
        self.check_agr_key(agr_dict)
        for _ in range(len(pq_agr_key_lst)):
            _, agr_key = heapq.heappop(pq_agr_key_lst)
            agr_item = agr_dict[agr_key]
            agr_class_lst = agr_item['agr_class_lst']
            agr_row_lst = list(agr_item['row_set'])

    def compute_agree_set(self, table_data):
        print('strip ptn')
        self.create_stripped_partitions(table_data)
        print('row eq class')
        self.compute_row_eq_class(table_data)        
        print('maximal eq class')
        maximal_class_lst = self.compute_maximal_eq_class(table_data)
        row_data = table_data['rows']
        agr_dict = {}
        for maximal_class in tqdm(maximal_class_lst):
            class_row_lst = list(maximal_class['row_set'])
            num_class_row = len(class_row_lst)
            union_row_class_set = set()
            for row in class_row_lst:
                union_row_class_set.update(row_data[row]['row_class_set'])
            union_row_class_lst = list(union_row_class_set)
            union_row_class_lst.sort()

            class_mask_lst = []
            for row in class_row_lst:
                row_class_set = row_data[row]['row_class_set']
                row_class_mask = [int(a in row_class_set) for a in union_row_class_lst]
                class_mask_lst.append(row_class_mask)
            
            class_set_mask = torch.tensor(class_mask_lst, dtype=torch.uint8) 
            agr_mask = torch.einsum('ij,jk->ikj', class_set_mask, class_set_mask.t())
            self.agr_mask_to_set(agr_dict, agr_mask, class_row_lst, union_row_class_lst)
        return agr_dict

    def agr_mask_to_set(self, agr_dict, agr_mask, class_row_lst, union_row_class_lst):
        num_tuple = agr_mask.shape[0]
        for i in range(num_tuple-1):
            for j in range(i+1, num_tuple):
                mask_array = agr_mask[i][j].numpy()
                agr_class_lst = [union_row_class_lst[offset] for offset, a in enumerate(mask_array) if a == 1] 
                agr_key = ','.join(agr_class_lst)
                if agr_key not in agr_dict:
                    agr_dict[agr_key] = {'agr_class_lst':agr_class_lst, 'row_set':set()}
                row_set = agr_dict[agr_key]['row_set']
                row_set.add(class_row_lst[i])
                row_set.add(class_row_lst[j])

    def create_agr_priority_queue(self, agr_dict):
        pq_item_lst = []
        for agr_key in agr_dict:
            agr_class_lst = agr_dict[agr_key]['agr_class_lst']
            agr_size = len(agr_class_lst)
            pq_item = (-agr_size, agr_key)
            pq_item_lst.append(pq_item)
        heapq.heapify(pq_item_lst)
        return pq_item_lst 

    def get_table_eq_classes(self, table_data):
        table_eq_class_dict = {}  
        col_data = table_data['columns']
        row_data = table_data['rows']
        for col, col_item in enumerate(col_data):
            eq_class_lst = col_item['eq_class_lst']
            for eq_class in eq_class_lst:
                rows = eq_class['rows']
                row_key = ','.join([str(r) for r in rows])
                if row_key not in table_eq_class_dict:
                    table_eq_class_dict[row_key] = eq_class
                
                eq_class['row_set'] = set(rows)
                del eq_class['rows']

        table_eq_classes = list(table_eq_class_dict.values())
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
                if left_set.issubset(right_set):
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
                row_lst = eq_class['rows']
                for row in row_lst:
                    row_item = row_data[row]
                    if 'row_class_set' not in row_item:
                        row_item['row_class_set'] = set() 
                    row_class_set = row_item['row_class_set']
                    row_class_info = f'{col}-{class_id}'
                    row_class_set.add(row_class_info)

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
                row_lst = ptn_dict[key]['rows']
                row_lst.append(row)

            eq_class_lst = []
            class_id = 0
            for key in ptn_dict:
                row_lst = ptn_dict[key]['rows']
                if len(row_lst) == 1:
                    continue
                eq_class = {
                    'col':col,
                    'id':class_id,
                    'rows':row_lst
                }
                class_id += 1
                eq_class_lst.append(eq_class)
            col_item['eq_class_lst'] = eq_class_lst

