Max_Title_Size = 60
Max_Col_Header_Size = 30
Max_Cell_Size = 100
Max_Header_Meta_Ratio = 0.2 # {sum of header meta tokens} / {window size}
MAX_WND_COLS = 20

# window size is determined by the encoder
def get_context_window_size(tokenizer):
    prefix = "title: context: " # the prefix is used by the encoder. There are 4 tokens 'title', ':', 'context', ':'
    tokens = tokenizer.tokenize(prefix)
    question_max_size = 50
    sep_token_size = 3 # used for question answering [CLS] [SEP] [SEP] 
    wnd_size = 512 - len(tokens) - question_max_size - sep_token_size # = 512 - 4 - 50 - 3 = 455 for user text 
    #for the encoder, max_seq_length (incuding prefix, user text, [cls] and [SEP]) = 455 + 4 + 2 = 461 
    return wnd_size 

def split_columns(tokenizer, table_data, wnd_size):
    col_data = table_data['columns']
    block_lst = []
    block_cols = []
    block_text = ''
    block_size = 0
    max_size = int(wnd_size * Max_Header_Meta_Ratio)
    for offset, col_info in enumerate(col_data):
        col_name = col_info['text'] 
        serial_text = col_name + ' ' + tokenizer.sep_token 
        serial_size = get_token_size(tokenizer, serial_text)
        if block_size + serial_size <= max_size:
            block_cols.append(offset)
            block_text += ' ' + serial_text
            block_size += serial_size
        else:
            block_info = {
                'cols':block_cols,
                'text':block_text,
                'size':block_size
            }
            block_lst.append(block_info)
            block_cols = [offset]
            block_text = serial_text
            block_size = serial_size

    if block_size > 0:
        block_info = {
            'cols':block_cols,
            'text':block_text,
            'size':block_size
        }
        block_lst.append(block_info)
    return block_lst

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
    assert text.strip() != ''
    if '.' not in text:
        return False
    if re.match(r'^-?\d+(?:\.\d+)$', text) is None:
        return False
    return True

def is_int(text):
    strip_text = text.strip()
    assert strip_text != ''
    if strip_text[0] in ['-', '+']:
        return strip_text[1].isdigit()
    else:
        return strip_text.isdigit()

def is_bool(text):
    assert text.strip() != ''
    if text.strip().lower() in ['true', 'false']:
        return True
    else:
        return False


