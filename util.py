Max_Title_Size = 60
Max_Col_Header_Size = 30
Max_Cell_Size = 100

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
    
