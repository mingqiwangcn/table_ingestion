eval "$(command conda 'shell.bash' 'hook' 2> /dev/null)"
conda create -y --name tab_gest python=3.8
conda activate tab_gest
conda install -y pytorch==2.2.1 torchvision==0.17.1 torchaudio==2.2.1 pytorch-cuda=12.1 -c pytorch -c nvidia
pip install -r requirements.txt