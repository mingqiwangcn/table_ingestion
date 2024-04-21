import torch
import torch.nn as nn
import transformers

def prune_layers(model):
    updated_layer = nn.ModuleList()
    for idx, module in enumerate(model.encoder.layer):
        if idx in [0]:
            updated_layer.append(module)
            break
    model.encoder.layer = updated_layer
    model.pooler = None

class SerialEncoder(nn.Module):
    def __init__(self, tokenizer_size):
        super().__init__()
        self.model = transformers.BertModel.from_pretrained('bert-base-uncased')
        prune_layers(self.model)
        self.model.resize_token_embeddings(tokenizer_size)

        '''
        input_d = 768
        self.emb_func = nn.Sequential(
                        nn.Linear(input_d, input_d),
                        nn.ReLU(),
                        nn.Dropout(),
                        nn.Linear(input_d, input_d),
                 )
        '''

    def forward(self, text_ids, cpr_emb):
        text_output = self.model(
            input_ids=text_ids
        )
        text_state = text_output['last_hidden_state']
        text_state_mean = torch.mean(text_state, dim=1)
        output = text_state_mean + cpr_emb  #self.emb_func(cpr_emb) 
        return output
