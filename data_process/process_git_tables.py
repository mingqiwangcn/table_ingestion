import pandas as pd
from multiprocessing import Pool as ProcessPool
import os
import glob
import random
from pathlib import Path
from tqdm import tqdm

def main():
    home_dir = os.path.expanduser("~")
    out_dir = os.path.join(home_dir, 'downloads', 'git_tables_sample')
    if not os.path.isdir(out_dir):
        os.makedirs(out_dir)
    csv_files = glob.glob(out_dir + '/*/*.csv')
    if len(csv_files) > 0:
        print('sample tables created already')
        return
    file_pattern = os.path.join(home_dir, 'downloads', 'git_tables', '*/*.parquet')
    file_lst = glob.glob(file_pattern)
    sample_files = random.sample(file_lst, 10000)
    for par_file in tqdm(sample_files):
        folder_name = Path(os.path.dirname(par_file)).stem
        df = pd.read_parquet(par_file)
        file_name = Path(par_file).stem
        out_folder = os.path.join(out_dir, folder_name)
        if not os.path.isdir(out_folder):
            os.makedirs(out_folder)
        out_file = os.path.join(out_folder, file_name + '.csv')
        df.to_csv(out_file)

    print('output to ' + out_dir)

        
if __name__ == '__main__':
    main()