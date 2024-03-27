from tqdm import tqdm
import util
from context_window import ContextWindow
from serial import TableSerializer
import time
import torch
import heapq
from code_book import CodeBook
from bin_packing import bin_pack
import random

class AgreeCodingSerializer(TableSerializer):
    def __init__(self):
        super().__init__()
        self.numeric_serializer = None
        self.serial_window.set_cell_code_book(CodeBook(self.tokenizer))

    def do_serialize(self, table_data):
        agr_set_lst = self.compute_agree_set(table_data)
        # Need to keep rows serialized already by previous agr_set
        row_serial_done_set = set()
        while len(agr_set_lst) > 0:
            disjoint_set_lst = self.compute_disjoint_groups(agr_set_lst)
            schema_block_lst = self.split_columns(disjoint_set_lst, table_data)
            for schema_block in schema_block_lst:
                yield from self.serialize_schema_block(table_data, schema_block, row_serial_done_set)
            self.remove_agr_sets(agr_set_lst, disjoint_set_lst)

    def remove_agr_sets(self, agr_group_lst, disjoint_group_lst):
        for group in disjoint_group_lst:
            agr_group_lst.remove(group)
  
    def serialize_schema_block(self, table_data, schema_block, row_serial_done_set):
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
  
    def sample_agr_set(self, table_data, agr_set_pct=0.3):
        row_data = self.get_row_data(table_data)
        num_agree_rows = 100
        num_rows_to_sample = int(num_agree_rows / agr_set_pct)
        M = min(num_rows_to_sample, len(row_data))
        sample_row_lst = random.sample(range(0, len(row_data)), M)
        row_item_lst = [row_data[a] for a in sample_row_lst]
        class_id_lst = [[a['class_id'] for a in row_item['cells']] for row_item in row_item_lst]
        data_class = torch.tensor(class_id_lst, dtype=torch.int)
        N_R,  N_C = data_class.shape 
        data_class_1 = data_class.view(N_R, -1, N_C).expand(-1, N_R, -1)
        data_class_2 = data_class.view(-1, N_R, N_C).expand(N_R, -1, -1)
        data_mask = (data_class_1 == data_class_2)
        agr_dict = {}
        for i in range(N_R - 1):
            for j in range(i+1, N_R):
                agr_cols = data_mask[i][j].nonzero().view(-1).numpy().tolist()
                if len(agr_cols) < 2:
                    continue
                agr_key = tuple(agr_cols)
                if agr_key not in agr_dict:
                    agr_dict[agr_key] = {'row_set':set()}
                agr_row_set = agr_dict[agr_key]['row_set']
                pair_row = [sample_row_lst[i], sample_row_lst[j]]
                agr_row_set.update(set(pair_row))
        
        # get top K agr-set cols to index table, they may overlap but it is ok
        top_agr_key_lst = self.compute_top_agr_sets(agr_dict, table_data)
        return top_agr_key_lst

    def compute_top_agr_sets(self, agr_dict, table_data):
        top_agr_key_lst= []
        key_set = set(agr_dict.keys())
        k = 0
        while k < len(agr_dict):
            if len(top_agr_key_lst) >= 3:
                break
            self.compute_agr_weight(key_set, agr_dict)
            item_lst = [(- agr_dict[a]['weight'], a) for a in key_set]
            heapq.heapify(item_lst)
            top_item = heapq.heappop(item_lst)
            top_key = top_item[1]
            top_agr_key_lst.append(top_key)
            key_set.remove(top_key)
            self.update_agr_row_set(top_key, key_set, agr_dict)
            k += 1
        return top_agr_key_lst
    
    def update_agr_row_set(self, top_key, key_set, agr_dict):
        top_row_set = agr_dict[top_key]['row_set']
        for key in key_set:
            agr_item = agr_dict[key]
            agr_item['row_set'] -= top_row_set

    def compute_agr_weight(self, key_set, agr_dict):
        for key in key_set:
            col_group = list(key)
            agr_item = agr_dict[key]
            row_set = agr_item['row_set']
            agr_item['weight'] = len(col_group) * len(row_set)
        

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

    def compute_disjoint_groups(self, agr_group_lst):
        item_lst = [(a, set(a)) for a in agr_group_lst]
        disjoint_item_lst = [item_lst[0]]
        N = len(item_lst)
        for offset in range(1, N):
            item = item_lst[offset]
            over_lapped = False
            for chosen_item in disjoint_item_lst:
                inter_set = item[1].intersection(chosen_item[1])
                if len(inter_set) > 0:
                    over_lapped = True
                    break
            if not over_lapped:
                disjoint_item_lst.append(item)
        
        disjoint_group_lst = [a[0] for a in disjoint_item_lst]
        return disjoint_group_lst

    # Use a single partition for all to share schema. 
    # Try to put agree groups in the same schema group
    def split_columns(self, agr_set_lst, table_data):
        col_data = self.get_col_data(table_data)
        col_max_set_dict = self.col_to_max_attr_set(agr_set_lst)
        Schema_Max_Size = int(self.serial_window.wnd_size * util.Max_Header_Meta_Ratio)
        group_item_lst = []
        fit_group_lst = self.get_fit_col_groups(col_data, col_max_set_dict, Schema_Max_Size)
        for col_group in fit_group_lst:
            serial_text, serial_size = self.get_column_serial(col_data, col_group)
            group_item = [col_group, serial_size, serial_text]
            group_item_lst.append(group_item)
        
        block_lst = []
        bin_lst = bin_pack(group_item_lst, Schema_Max_Size)
        for bin_entry in bin_lst:
            col_group_lst = [a[0] for a in bin_entry.item_lst]
            block_text = ''.join([a[2] for a in bin_entry.item_lst])
            block_size = sum([a[1] for a in bin_entry.item_lst])
            block_info = self.get_block_info(col_group_lst, block_text, block_size)
            block_lst.append(block_info)
        return block_lst 

    def get_fit_col_groups(self, col_data, col_max_set_dict, max_size):
        fit_group_lst = []
        block_col_set = set()
        for col, _ in enumerate(col_data):
            if col in block_col_set:
                continue # col may be already included by attr set
            col_group = None
            if col in col_max_set_dict:
                attr_set = col_max_set_dict[col]
                col_group = list(attr_set)
                col_group.sort()
                sub_group_lst = self.split_col_group(col_data, col_group, max_size)
                fit_group_lst.extend(sub_group_lst)
            else:
                col_group = [col]
                fit_group_lst.append(col_group)
            block_col_set.update(set(col_group))
        return fit_group_lst

    def split_col_group(self, col_data, col_group, max_size):
        group_size = sum([col_data[col]['size'] + 1 for col in col_group])
        if group_size < max_size:
            return [col_group]
        sub_group_lst = []
        sub_group = []
        sub_group_size = 0
        for col in col_group:
            serial_size = col_data[col]['size'] + 1
            if sub_group_size + serial_size > max_size:
                assert len(sub_group) > 0
                sub_group_lst.append(sub_group)
                sub_group = []
                sub_group_size = 0
            sub_group.append(col)
            sub_group_size += serial_size
        if len(sub_group) > 0:
            sub_group_lst.append(sub_group)
        return sub_group_lst

    def get_block_info(self, col_group_lst, block_text, block_size):
        updated_block_text = block_text.rstrip()[:-1] + self.tokenizer.sep_token
        block_info = {
            'cols':col_group_lst,
            'text':updated_block_text,
            'size':block_size
        }
        return block_info

    def compute_agree_set(self, table_data):
        self.create_partitions(table_data)
        agr_set_lst = self.sample_agr_set(table_data)
        return agr_set_lst

    def index_by_agr_set(self, table_data, agr_set_lst):
        row_data  = self.get_row_data(table_data)
        for col_group in agr_set_lst:
            agr_dict = {}
            for row, row_item in enumerate(row_data):
                row_cells = row_item['cells']
                agr_class_lst = [row_cells[col]['class_id'] for col in col_group]
                agr_key = tuple(agr_class_lst)
                if agr_key not in agr_dict:
                    agr_dict[agr_key] = {'rows':[]}
                agr_rows = agr_dict[agr_key]['rows']
                agr_rows.append(row)
            
            col_name_lst = self.show_col_names(table_data, col_group)
            print('sample ', sample_number, 'col groups', col_name_lst, 
                  'total rows =', len(row_data), 'agr sets', len(agr_dict))
    
    def show_col_names(self, table_data, col_group):
        col_data = self.get_col_data(table_data)
        col_name_lst = [col_data[col]['text'] for col in col_group]
        return col_name_lst

    def get_col_data(self, table_data):
        return table_data['columns']

    def get_row_data(self, table_data):
        return table_data['rows']

    def create_partitions(self, table_data):
        col_data = self.get_col_data(table_data)
        row_data = self.get_row_data(table_data)
        for col, col_item in enumerate(col_data):
            ptn_dict = {}
            class_id = 0
            for row, row_item in enumerate(row_data):
                cell_info = row_data[row]['cells'][col]
                cell_text = cell_info['text']
                key = util.get_hash_key(cell_text)
                if key not in ptn_dict:
                    ptn_dict[key] = {'class_id':class_id}
                    class_id += 1
                row_class_id = ptn_dict[key]['class_id']
                row_item['cells'][col]['class_id'] = row_class_id
            
