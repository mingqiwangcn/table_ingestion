import transformers
import src.model


def get_tokenizer():
    tokenizer = transformers.BertTokenizerFast.from_pretrained('bert-base-uncased')
    return tokenizer

def get_encode_model(model_path, device):
    model = src.model.Retriever.from_pretrained(model_path) 
    model.eval()
    model = model.to(device)
    model = model.half() #fp16
    return model

