import transformers
import src.model

Max_Title_Size = 60
Max_Col_Header_Size = 30
Max_Cell_Size = 100

def get_encode_model(model_path, device):
    model = src.model.Retriever.from_pretrained(model_path) 
    model.eval()
    model = model.to(device)
    model = model.half() #fp16
    return model

def truncate(tokenizer, text, max_size):
    tokens = tokenizer.tokenize(text)
    if len(tokens > max_size):
        updated_tokens = tokens[:max_size]
        updated_text = tokenize.decode(tokenizer.convert_tokens_to_ids(updated_tokens))
    else:
        updated_text = text
    return updated_text

def preprocess_schema(tokenizer, table_data):
    title = table_data['document_title']
    table_data['document_title'] = truncate(tokenizer, title, Max_Title_Size) 

    col_data = table_data['columns']
    for col_info in cell_data:
        text = col_info['text']
        col_info['text'] = truncate(tokenizer, text, Max_Col_Header_Size)

def preprocess_row(row_item):
    cell_lst = row_item['cells']
    for cell_info in cell_lst:
        text = cell_info['text']
        cell_info['text'] = truncate(tokenizer, text, Max_Cell_Size)
    
