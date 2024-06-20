import pandas as pd
import os
import glob
import random
from pathlib import Path
from tqdm import tqdm
import mysql.connector
import sqlalchemy as db

def main():
    conn = db.create_engine("mysql+mysqlconnector://sa:sa_123@localhost/chembl_34")
    table_lst = get_table_lst(conn)
    
    out_dir = './output/chembl'
    if not os.path.isdir(out_dir):
        os.makedirs(out_dir)
    
    csv_files = glob.glob(out_dir + '/*.csv')
    if len(csv_files) > 0:
        print('tables created already')
        return

    for table_name in tqdm(table_lst):
        sql = 'SELECT * FROM ' + table_name + ' LIMIT 10000'
        df = pd.read_sql(sql, con=conn)
        out_file = os.path.join(out_dir, table_name + '.csv')
        df.to_csv(out_file, index=False)

    print('output to ' + out_dir)

def get_table_lst(conn):
    table_lst = []
    df = pd.read_sql("show tables", con=conn)
    data = df.values.tolist()
    for item in data:
        table_name = item[0]
        table_lst.append(table_name)
    return table_lst

if __name__ == '__main__':
    main()