from tqdm import tqdm
import util
from context_window import ContextWindow
from serial import TableSerializer
import time
import torch
import heapq
from code_book import CodeBook
from bin_packing import bin_pack

class AgreeCodingSerializer(TableSerializer):
    def __init__(self):
        super().__init__()
        self.numeric_serializer = None
        self.serial_window.set_cell_code_book(CodeBook(self.tokenizer))

    def do_serialize(self, table_data):
        agr_dict = self.compute_agree_set(table_data)
        attr_group_dict = self.compute_agr_attrs(agr_dict, table_data)
        schema_block_lst = self.split_columns(attr_group_dict, table_data)
        self.assign_agr_key_to_schema_block(schema_block_lst, agr_dict)
        for schema_block in schema_block_lst:
            yield from self.serialize_schema_block(table_data, schema_block, agr_dict, attr_group_dict)
  
    def assign_agr_key_to_schema_block(self, schema_block_lst, agr_dict):
        agr_key_to_assign_set = set(agr_dict.keys())
        for schema_block in schema_block_lst:
            block_agr_map = {}
            schema_block['agr_map'] = block_agr_map
            schema_col_group_lst = schema_block['cols']
            schema_col_lst = []
            for col_group in schema_col_group_lst:
                schema_col_lst.extend(col_group)
            schema_col_set = set(schema_col_lst)
            exact_matched_key_lst = []
            for key in agr_key_to_assign_set:
                agr_item = agr_dict[key]
                agr_class_lst = agr_item['agr_class_lst']
                agr_col_group = self.agr_class_to_col_group(agr_class_lst)
                agr_col_set = set(agr_col_group)
                inter_set = agr_col_set.intersection(schema_col_set)
                if len(inter_set) == 0:
                    continue
                matched_offset = None
                if agr_col_group in schema_col_group_lst:
                    matched_offset = schema_col_group_lst.index(agr_col_group)
                    exact_matched_key_lst.append(key)
                block_agr_map[key] = {'matched_offset':matched_offset} 
            
            for matched_key in exact_matched_key_lst:
                agr_key_to_assign_set.remove(matched_key)

    def serialize_schema_block(self, table_data, schema_block, agr_dict, attr_group_dict):
        schema_text = self.get_window_schema_text(table_data, schema_block)
        self.serial_window.set_schema_text(schema_text)
        yield from self.get_wnd_block(table_data, schema_block, agr_dict, attr_group_dict)
  
    def get_window_schema_text(self, table_data, schema_block):
        schema_text = table_data['documentTitle'] + ' ' + self.tokenizer.sep_token + ' ' + schema_block['text'] + ' '
        return schema_text

    def get_serial_text(self, table_data, row, col_group_lst, agr_group_offset):
        col_data = table_data['columns']
        row_cells = table_data['rows'][row]['cells']
     
        row_serial_cell_lst = []
        for offset, col_group in enumerate(col_group_lst):
            if offset == agr_group_offset:
                first_col = col_group[0]
                cell_info = row_cells[first_col]
                row_serial_cell_lst.append(cell_info)
                text_key = 'group_text'
                size_key = 'group_size'
                cell_info[text_key] = ' & '.join([row_cells[col]['text'] for col in col_group])
                cell_info[size_key] = sum([row_cells[col]['size'] for col in col_group]) + len(col_group) - 1   
                cell_info['group_size'] = len(col_group)

                cell_text, cell_size = self.get_cell_text(cell_info, text_key, size_key)
            
                cell_serial_text = cell_text + ' | '
                cell_serial_size = cell_size + 1 
                cell_info['serial_text'] = cell_serial_text
                cell_info['serial_size'] = cell_serial_size
                self.update_related_cell(cell_info)
            else:
                text_key = 'text'
                size_key = 'size'
                for col in col_group:
                    cell_info = row_cells[col]
                    row_serial_cell_lst.append(cell_info)
                    cell_text, cell_size = self.get_cell_text(cell_info, text_key, size_key)
                    cell_serial_text = cell_text + ' | '
                    cell_serial_size = cell_size + 1 
                    cell_info['serial_text'] = cell_serial_text
                    cell_info['serial_size'] = cell_serial_size
                    self.update_related_cell(cell_info)
       
        boundary_cell = row_serial_cell_lst[-1]
        boundary_cell['serial_text'] = boundary_cell['serial_text'].rstrip()[:-1] + ' ' + self.tokenizer.sep_token + ' '
        return row_serial_cell_lst

    def get_cell_text(self, cell_info, text_key, size_key):
        text = cell_info[text_key]
        if cell_info[size_key] < 2:
            return text, cell_info[size_key]

        code, code_size = self.serial_window.cell_code_book.get_code(cell_info, text_key=text_key, size_key=size_key)
        return code, code_size

    def update_related_cell(self, cell_info):
        compress_code = cell_info.get('compress_code', None)
        if compress_code is not None:
            serial_text = cell_info['serial_text']
            serial_size = cell_info['serial_size']
            pre_cell_lst = cell_info['pre_cells']
            for pre_cell in pre_cell_lst:
                pre_cell['updated_serial_text'] = serial_text
                pre_cell['updated_serial_size'] = serial_size

    def get_group_keys(self, col_group_lst, attr_group_dict):
        group_matched_keys = []
        for col_group in col_group_lst:
            group_key = tuple(col_group)
            if group_key in attr_group_dict:
                group_matched_keys.extend(attr_group_dict[group_key]['agr_keys'])
        return group_matched_keys

    def get_wnd_block(self, table_data, schema_block, agr_dict, attr_group_dict):
        col_group_lst = schema_block['cols']
        block_agr_map = schema_block['agr_map']

        group_matched_keys = self.get_group_keys(col_group_lst, attr_group_dict)
        serial_agr_key_lst = self.create_agr_priority_queue(agr_dict, group_matched_keys, list(block_agr_map.keys()))
       
        row_serial_done_set = set()
        for _ in range(len(serial_agr_key_lst)):
            _, agr_key = heapq.heappop(serial_agr_key_lst)
            agr_item = agr_dict[agr_key]
            row_set_to_serial = agr_item['row_set'] - row_serial_done_set
            row_lst = list(row_set_to_serial)
            row_serial_done_set.update(row_set_to_serial)
            agr_map = block_agr_map[agr_key]
            agr_group_offset = agr_map['matched_offset']
            yield from self.serialize_on_row(table_data, row_lst, col_group_lst, agr_group_offset)

        row_data = table_data['rows']
        table_row_set = set(range(len(row_data)))
        other_row_lst = list(table_row_set - row_serial_done_set)
        if len(other_row_lst) > 0:
            yield from self.serialize_on_row(table_data, other_row_lst, col_group_lst, None)

        if self.serial_window.can_pop():
            serial_block = self.serial_window.pop(table_data)
            yield serial_block

    def try_serialize_row(self, table_data, row, col_group_lst, agr_group_offset):
        row_serial_cell_lst = self.get_serial_text(table_data, row, col_group_lst, agr_group_offset)
        status = self.serial_window.can_add(table_data, row, col_group_lst, row_serial_cell_lst)
        return status

    def serialize_on_row(self, table_data, row_lst, col_group_lst, agr_group_offset):
        for row in row_lst:
            fit_ok, serial_info = self.try_serialize_row(table_data, row, col_group_lst, agr_group_offset)
            if fit_ok: 
                self.serial_window.add(table_data, serial_info)
            else:
                serial_block = self.serial_window.pop(table_data)
                yield serial_block
                fit_ok, serial_info = self.try_serialize_row(table_data, row, col_group_lst, agr_group_offset)
                assert(fit_ok)
                self.serial_window.add(table_data, serial_info)

    def get_max_class_size(self, agr_key_lst, agr_dict):
        max_size = 0
        for key in agr_key_lst:
            agr_class_lst = agr_dict[key]['agr_class_lst']
            class_size = len(agr_class_lst)
            if class_size > max_size:
                max_size = class_size
        return max_size

    def create_agr_priority_queue(self, agr_dict, group_matched_keys, agr_key_lst):
        group_matched_key_set = set(group_matched_keys)
        max_class_size = self.get_max_class_size(agr_key_lst, agr_dict)
        pq_item_lst = []
        for agr_key in agr_key_lst:
            agr_class_lst = agr_dict[agr_key]['agr_class_lst']
            agr_size = len(agr_class_lst)
            if agr_key in group_matched_key_set:
                agr_size = max_class_size + 1

            pq_item = (-agr_size, agr_key)
            pq_item_lst.append(pq_item)
        heapq.heapify(pq_item_lst)
        return pq_item_lst

    def agr_class_to_col_group(self, agr_class_lst):
        col_group = []
        for agr_class in agr_class_lst:
            col = int(agr_class.split('-')[0])
            col_group.append(col)
        return col_group

    def get_col_groups(self, agr_col_group, table_data):
        agr_col_set = set(agr_col_group)
        col_data = table_data['columns']
        col_group_lst = []
        for col, _ in enumerate(col_data):
            if col in agr_col_set:
                if col == agr_col_group[0]:
                    col_group = agr_col_group
            else:
                col_group = [col]
            col_group_lst.append(col_group)
        return col_group_lst
  
    def compute_agr_attrs(self, agr_dict, table_data):
        attr_group_dict = {}
        row_data = table_data['rows']
        for key in agr_dict:
            agr_item = agr_dict[key]
            agr_class_lst = agr_item['agr_class_lst']
            row_set = agr_item['row_set']
            num_rows = len(row_set)
            rep_row = next(iter(row_set)) 
            size_lst = []
            col_lst = []
            for agr_class in agr_class_lst:
                col = int(agr_class.split('-')[0])
                col_lst.append(col)
                size = row_data[rep_row]['cells'][col]['size']
                size_lst.append(size)

            agr_size = sum(size_lst) * num_rows
            attr_tuple = tuple(col_lst)
            if attr_tuple not in attr_group_dict:
                attr_group_dict[attr_tuple] = {'group':col_lst, 'agr_keys':[], 'row_set':set(), 'agr_size':0}

            attr_group = attr_group_dict[attr_tuple]
            attr_group['agr_size'] += agr_size
            attr_group['row_set'].update(row_set)
            attr_group['agr_keys'].append(key) 

        for group_key in attr_group_dict:
            group_item = attr_group_dict[group_key]
            group_item['row_percent'] = len(group_item['row_set']) / len(row_data)
            del group_item['row_set']
        return attr_group_dict

    def compute_max_disjoint_attr_sets(self, attr_group_dict):
        attr_set_lst = []  
        for key in attr_group_dict:
            attr_group = attr_group_dict[key]
            group = attr_group['group']
            if len(group) < 2:
                continue
            attr_set = (set(group), attr_group['agr_size'])
            attr_set_lst.append(attr_set)
        
        max_disjoint_sets = util.set_packing(attr_set_lst)
        return max_disjoint_sets

    def col_to_max_attr_set(self, attr_set_lst):
        col_set_dict = {}
        for attr_set in attr_set_lst:
            for col in attr_set:
                col_set_dict[col] = attr_set
        return col_set_dict
    
    def get_column_serial(self, col_data, col_group):
        col_name_lst = [col_data[a]['text'] for a in col_group] 
        serial_text = ' & '.join(col_name_lst) + ' | '
        serial_size = sum([col_data[a]['size'] for a in col_group]) + len(col_name_lst) 
        return (serial_text, serial_size)

    # Use a single partition for all to share schema. 
    # Try to put agree groups in the same schema group
    def split_columns(self, attr_group_dict, table_data):
        col_data = table_data['columns']
        block_lst = []
        col_group_lst = [] 
        block_col_set = set()
      
        max_disjoint_attr_sets = self.compute_max_disjoint_attr_sets(attr_group_dict)
        col_max_set_dict = self.col_to_max_attr_set(max_disjoint_attr_sets)
      
        group_item_lst = []
        Schema_Max_Size = int(self.serial_window.wnd_size * util.Max_Header_Meta_Ratio)
        for col, _ in enumerate(col_data):
            if col in block_col_set:
                continue # col may be already included by attr set
            col_group = []
            if col in col_max_set_dict:
                attr_set = col_max_set_dict[col]
                col_group = list(attr_set)
                col_group.sort()
            else:
                col_group = [col]
            
            block_col_set.update(set(col_group))
            serial_text, serial_size = self.get_column_serial(col_data, col_group)
            
            group_item = [col_group, serial_size, serial_text]
            group_item_lst.append(group_item)
        
        bin_lst = bin_pack(group_item_lst, Schema_Max_Size)
        for bin_entry in bin_lst:
            col_group_lst = [a[0] for a in bin_entry.item_lst]
            block_text = ''.join([a[2] for a in bin_entry.item_lst])
            block_size = sum([a[1] for a in bin_entry.item_lst])
            block_info = self.get_block_info(col_group_lst, block_text, block_size)
            block_lst.append(block_info)
        return block_lst 

    def get_block_info(self, col_group_lst, block_text, block_size):
        updated_block_text = block_text.rstrip()[:-1] + self.tokenizer.sep_token
        block_info = {
            'cols':col_group_lst,
            'text':updated_block_text,
            'size':block_size
        }
        return block_info

    def check_agr_key(self, agr_dict):
        key_lst = list(agr_dict.keys())

        N = len(key_lst)
        pair_dict = {}
        for i in range(N):
            key_1 = key_lst[i]
            agr_class_1 = set(agr_dict[key_1]['agr_class_lst']) 
            set_1 = agr_dict[key_1]['row_set']
            for j in range(N):
                if i == j:
                    continue

                pair = (i, j) if i < j else (j, i)
                if pair in pair_dict:
                    continue
                
                pair_dict[pair] = True
                key_2 = key_lst[j]
                agr_class_2 = set(agr_dict[key_2]['agr_class_lst']) 
                set_2 = agr_dict[key_2]['row_set']
                
                if len(set_1.intersection(set_2)) > 1:
                    if agr_class_1.issubset(agr_class_2):
                        key_msg = key_1 + '   ' + key_2
                    else:
                        key_msg = key_2 + '   ' + key_1
                    print(key_msg)

    def sort_class(self, row_class_lst):
        item_lst = []
        for row_class in row_class_lst:
            parts = row_class.split('-') 
            col = int(parts[0])
            class_id = int(parts[1])
            item = (col, class_id, row_class)
            item_lst.append(item)

        sorted_item_lst = sorted(item_lst, key=lambda x: (x[0], x[1]))
        sorted_row_class_lst = [a[2] for a in sorted_item_lst]
        return sorted_row_class_lst

    def compute_agree_set(self, table_data):
        self.create_stripped_partitions(table_data)
        self.compute_row_eq_class(table_data)        
        maximal_class_lst = self.compute_maximal_eq_class(table_data)
        row_data = table_data['rows']
        agr_dict = {}
        for maximal_class in tqdm(maximal_class_lst):
            class_row_lst = list(maximal_class['row_set'])
            num_class_row = len(class_row_lst)
            union_row_class_set = set()
            for row in class_row_lst:
                union_row_class_set.update(row_data[row]['row_class_set'])
            union_row_class_lst = self.sort_class(list(union_row_class_set))

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
        tuple_dict = {}
        num_tuple = agr_mask.shape[0]
        for i in range(num_tuple-1):
            for j in range(i+1, num_tuple):
                mask_array = agr_mask[i][j].numpy()
                tuple_key = tuple(mask_array)
                if tuple_key not in tuple_dict:
                    tuple_dict[tuple_key] = {'row_set':set()}
                row_set = tuple_dict[tuple_key]['row_set']
                row_set.add(class_row_lst[i])
                row_set.add(class_row_lst[j])
       
        for tuple_key in tuple_dict:
            tuple_row_set = tuple_dict[tuple_key]['row_set']
            tuple_mask = list(tuple_key)
            agr_class_lst = [union_row_class_lst[offset] for offset, a in enumerate(tuple_mask) if a == 1] 
            agr_key = ','.join(agr_class_lst)
            if agr_key not in agr_dict:
                agr_dict[agr_key] = {'agr_class_lst':agr_class_lst, 'row_set':tuple_row_set}
            else:    
                agr_row_set = agr_dict[agr_key]['row_set']
                agr_row_set.update(tuple_row_set)

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

