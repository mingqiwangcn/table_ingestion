import torch
import torch.nn as nn

class SerialEncoder(nn.Module):
    def __init__(self):
        super().__init__()
        input_d = 768
        self.func = nn.Sequential(
                        nn.Linear(input_d, input_d),
                        nn.ReLU(),
                        nn.Dropout(),
                        nn.Linear(input_d, input_d),
                        nn.ReLU(),
                        nn.Dropout(),
                        nn.Linear(input_d, input_d)
                 )
    def forward(self, input_emb):
        out_emb = self.func(input_emb)
        return out_emb