import torch
import torch.nn as nn
import transformers

class SerialEncoder(nn.Module):
    def __init__(self, model):
        super().__init__()
        self.model = transformers.BertModel.from_pretrained('bert-base-uncased')

        input_d = 768
        self.merge_func = nn.Sequential(
                        nn.Linear(input_d * 3, input_d),
                        nn.ReLU(),
                        nn.Dropout(),
                        nn.Linear(input_d, input_d),
                 )
        
    def get_text_emb(self, text_ids):
        text_output = self.model(
            input_ids=text_ids
        )
        text_state = text_output[0]
        text_state_mean = torch.mean(text_state, dim=1)
        output = text_state_mean 
        return output

    def forward(self, passage_text_ids, schema_text_ids, cpr_emb):
        p_emb = self.get_text_emb(passage_text_ids)
        scm_emb = self.get_text_emb(schema_text_ids)
        merge_emb = torch.cat([p_emb, scm_emb, cpr_emb], dim=1)
        merge_out = self.merge_func(merge_emb)
        return merge_out
        