import re
import numpy as np

Max_Title_Size = 60
Max_Col_Header_Size = 30
Max_Cell_Size = 100
Max_Header_Meta_Ratio = 0.2 # {sum of header meta tokens} / {window size}
MAX_WND_COLS = 20

class CellDataType:
    INT = 1
    BOOL = 2
    FLOAT = 3

# window size is determined by the encoder
def get_context_window_size(tokenizer):
    prefix = "title: context: " # the prefix is used by the encoder. There are 4 tokens 'title', ':', 'context', ':'
    tokens = tokenizer.tokenize(prefix)
    question_max_size = 50
    sep_token_size = 3 # used for question answering [CLS] [SEP] [SEP] 
    wnd_size = 512 - len(tokens) - question_max_size - sep_token_size # = 512 - 4 - 50 - 3 = 455 for user text 
    #for the encoder, max_seq_length (incuding prefix, user text, [cls] and [SEP]) = 455 + 4 + 2 = 461 
    return wnd_size 

def wrap_text(text):
    if len(text.split()) > 1:
        text = '"' + text + '"'
    return text

def get_token_size(tokenizer, text):
    return len(tokenizer.tokenize(text))

def truncate(tokenizer, text, max_size):
    tokens = tokenizer.tokenize(text)
    if len(tokens) > max_size:
        updated_tokens = tokens[:max_size]
        updated_text = tokenizer.decode(tokenizer.convert_tokens_to_ids(updated_tokens))
    else:
        updated_text = text
    return wrap_text(updated_text)

def preprocess_schema(tokenizer, table_data):
    title_key = 'documentTitle'
    title = table_data[title_key].strip()
    table_data[title_key] = truncate(tokenizer, title, Max_Title_Size)
    table_data['title_size'] = len(tokenizer.tokenize(table_data[title_key]))

    col_data = table_data['columns']
    for col_info in col_data:
        text = col_info['text'].strip()
        col_info['text'] = truncate(tokenizer, text, Max_Col_Header_Size)

def preprocess_row(tokenizer, row_item):
    cell_lst = row_item['cells']
    for cell_info in cell_lst:
        text = cell_info['text'].strip()
        cell_info['text'] = truncate(tokenizer, text, Max_Cell_Size)

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
        return strip_text[1].isdigit()
    else:
        return strip_text.isdigit()

def is_bool(text):
    if text.strip().lower() in ['true', 'false']:
        return True
    else:
        return False

def infer_col_type(table_data):
    col_data = table_data['columns']
    row_data = table_data['rows']
    for col, col_info in enumerate(col_data):
        type_lst = []
        for row_item in row_data:
            cell_info = row_item['cells'][col]
            cell_text = cell_info['text']
            infer_type = None
            if (cell_text != ''):
                if is_bool(cell_text):
                    infer_type = CellDataType.BOOL
                elif is_float(cell_text):
                    if is_int(cell_text):
                        infer_type = CellDataType.INT
                    else:
                        infer_type = CellDataType.FLOAT
                else:
                    infer_type = None
                if infer_type is not None:
                    cell_info['infer_type'] = infer_type
                    type_lst.append(infer_type)

        if len(type_lst) > 0:
            type_arr = np.array(type_lst)
            if all(type_arr == CellDataType.BOOL):
                col_info['infer_type'] = CellDataType.BOOL
            elif all(type_arr == CellDataType.INT):
                col_info['infer_type'] = CellDataType.INT
            elif all([a in (CellDataType.FLOAT, CellDataType.INT) for a in type_lst]):
                col_info['infer_type'] = CellDataType.FLOAT

