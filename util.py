import re
import numpy as np
import math

Max_Title_Size = 60
Max_Col_Header_Size = 30
Max_Cell_Size = 30
Max_Header_Meta_Ratio = 0.1 # {sum of header meta tokens} / {window size}
Max_Seq_Length = 511 # 461

class CellDataType:
    INT = 1
    BOOL = 2
    FLOAT = 3
    POLYGON = 4

# window size is determined by the encoder
def get_context_window_size(tokenizer):
    prefix = "title: context: " # the prefix is used by the encoder. There are 4 tokens 'title', ':', 'context', ':'
    tokens = tokenizer.tokenize(prefix)
    question_max_size = 0 # 50 , Not consider it right now
    sep_token_size = 3 # used for question answering [CLS] [SEP] [SEP] 
    wnd_size = 512 - len(tokens) - question_max_size - sep_token_size # = 512 - 4 - 0 - 3 = 505 for user text 
    #for the encoder, max_seq_length (incuding prefix, user text, [cls] and [SEP]) = 505 + 4 + 2 = 511 
    return wnd_size 

def wrap_text(text, size):
    if len(text.split()) > 1:
        text = '"' + text + '"'
        size += 2
    return text, size 

def get_hash_key(text):
    key = text.strip().lower()
    return key

def get_token_size(tokenizer, text):
    return len(tokenizer.tokenize(text))

def truncate_table(tokenizer, text_lst, max_size):
    batch_encoding = tokenizer(text_lst,
                               truncation=True, 
                               max_length=max_size, 
                               add_special_tokens=False,
                               return_token_type_ids=False,
                               return_attention_mask=False,
                               return_length=True
                               )
   
    input_id_lst = batch_encoding['input_ids']
    token_size_lst = batch_encoding['length']
    
    out_text_lst = text_lst
    decode_offset_lst = [offset for offset, size in enumerate(token_size_lst) if size == max_size]
    if len(decode_offset_lst) > 0: 
        decode_input_lst =[input_id_lst[offset] for offset in decode_offset_lst] 
        decode_text_lst = tokenizer.batch_decode(decode_input_lst)
        out_text_lst = []
        offset_map = {}
        for pos, offset in enumerate(decode_offset_lst):
            offset_map[offset] = pos
        for offset, _ in enumerate(text_lst):
            if offset not in offset_map:
                out_text_lst.append(text_lst[offset])
            else:
                decode_pos = offset_map[offset]
                out_text_lst.append(decode_text_lst[decode_pos])

    return out_text_lst, token_size_lst

def preprocess_schema(tokenizer, table_data):
    title_key = 'documentTitle'
    title = table_data[title_key].strip()
    title_info = truncate_table(tokenizer, [title], Max_Title_Size)
    table_data[title_key] = title_info[0][0]
    table_data['title_size'] = title_info[1][0]
    
    col_data = table_data['columns']
    text_lst = [col_info['text'].strip() for col_info in col_data]
    out_text_lst, token_size_lst = truncate_table(tokenizer, text_lst, Max_Col_Header_Size)
    for offset, col_info in enumerate(col_data):
        col_info['text'] = out_text_lst[offset]
        col_info['size'] = token_size_lst[offset]

    serial_row_col = table_data.get('serial_row_col', None)
    if serial_row_col is None:
        row_cnt = len(table_data['rows'])
        preprocess_row(tokenizer, table_data, 0, row_cnt) # may use batch in case of large rows.
    else:
        for row, _ in serial_row_col:
            preprocess_row(tokenizer, table_data, row, row + 1)

def preprocess_row(tokenizer, table_data, start_row, end_row):
    col_num = len(table_data['columns'])
    row_data = table_data['rows']
    row_num = len(row_data)
    for col in range(col_num):
        text_dict = {}
        for row in range(start_row, end_row):
            cell_info = row_data[row]['cells'][col]
            cell_info['text'] = cell_info['text'].strip()
            cell_text = cell_info['text']
            if cell_text != '':
                key = get_hash_key(cell_text)
                if key not in text_dict:
                    text_dict[key] = {'text':cell_text, 'rows':[]}
                row_lst = text_dict[key]['rows']
                row_lst.append(row)
            else:
                cell_info['size'] = 0

        key_lst = list(text_dict.keys())
        if len(key_lst) > 0:
            text_lst = [text_dict[key]['text'] for key in key_lst]
            out_text_lst, token_size_lst = truncate_table(tokenizer, text_lst, Max_Cell_Size)
            for offset, key in enumerate(key_lst):
                out_text = out_text_lst[offset]
                token_size = token_size_lst[offset]
                row_lst = text_dict[key]['rows']
                for row in row_lst:
                    cell_info = row_data[row]['cells'][col]
                    cell_info['text'] = out_text
                    cell_info['size'] = token_size

def is_float(text):
    strip_text = text.strip()
    if '.' not in strip_text:
        return False
    if re.match(r'^-?\d+(?:\.\d+)$', strip_text) is None:
        return False
    return True

def is_int(text):
    strip_text = text.strip()
    if strip_text == '':
        return False
    if strip_text[0] in ['-', '+']:
        if len(strip_text) > 1:
            return strip_text[1].isdigit()
        else:
            return False
    else:
        return strip_text.isdigit()

def is_bool(text):
    if text.strip().lower() in ['true', 'false', 't', 'f']:
        return True
    else:
        return False

def is_polygon(text):
    text = text.strip().lower()
    if text == '':
        return False
    if text[0] == '"' and text[-1] == '"':
        text = text[1:-1]
    polygon_sub_str = 'multipolygon'
    if text.startswith(polygon_sub_str):
        text = text.replace(polygon_sub_str, '')
        for ch in text:
            if not (ch in ['(', ')', ',' , ' ', '.'] or ch.isdigit()):
                return False
        return True
    else:
        return False

def is_prime(N):
    if N <= 1:
        return False
    sqrt_num = int(math.sqrt(N))
    for i in range(2, sqrt_num + 1):
        if N % i == 0:
            return False
    return True

def infer_col_type(table_data):
    col_data = table_data['columns']
    row_data = table_data['rows']
    for col, col_info in enumerate(col_data):
        type_lst = []
        bool_count = 0
        int_count = 0
        float_count = 0
        polygon_count = 0
        for row_item in row_data:
            if (bool_count >= 3) or (int_count >= 3) or (float_count >= 3) or (polygon_count >= 1):
                break
            cell_info = row_item['cells'][col]
            cell_text = cell_info['text']
            infer_type = None
            if (cell_text != ''):
                if is_bool(cell_text):
                    infer_type = CellDataType.BOOL
                    bool_count += 1
                elif is_int(cell_text):
                    infer_type = CellDataType.INT
                    int_count += 1
                elif is_float(cell_text):
                    infer_type = CellDataType.FLOAT
                    float_count += 1
                elif is_polygon(cell_text):
                    infer_type = CellDataType.POLYGON
                    polygon_count += 1
                else:
                    infer_type = None
                if infer_type is not None:
                    cell_info['infer_type'] = infer_type
                    type_lst.append(infer_type)

        if len(type_lst) > 0:
            type_arr = np.array(type_lst)
            if bool_count >= 3:
                col_info['infer_type'] = CellDataType.BOOL
            elif int_count >= 3: 
                col_info['infer_type'] = CellDataType.INT
            elif float_count >= 3: 
                col_info['infer_type'] = CellDataType.FLOAT
            elif polygon_count >= 1:
                col_info['infer_type'] = CellDataType.POLYGON
            
# given a list of sets with weights, find pair-wise disjoit sets such that the total weights maximized  
# each element of set_lst is a tuple (set, weight, key)
def set_packing(set_lst):
    rw_set_lst = []
    for set_item in set_lst:
        input_set = set_item[0] 
        weight = set_item[1]
        rw = weight / math.sqrt(len(input_set))
        rw_set = (input_set, rw, set_item[2])
        rw_set_lst.append(rw_set)
    sorted_set_lst = sorted(rw_set_lst, key=lambda x:x[1], reverse=True)
    pd_set_lst = []
    for set_item in sorted_set_lst:
        input_set = set_item[0]
        over_lapped = False
        for pd_set in pd_set_lst:
            sub_set = pd_set[0].intersection(input_set) 
            if len(sub_set) > 0:
                over_lapped = True
                break
        if not over_lapped:
            pd_set_lst.append(set_item)
    return pd_set_lst
