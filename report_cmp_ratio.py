import json
import pandas as pd

def read_stat(dataset, strategy):
    stat_file = f'./output/{dataset}/{strategy}/stat.json'
    with open(stat_file) as f:
        stat_dict = json.load(f)
    return stat_dict

def main():
    dataset = 'nyc_open_100K'
    schema_stat_dict = read_stat(dataset, 'schema')
    compress_stat_dict = read_stat(dataset, 'compress')
    assert len(schema_stat_dict) == len(compress_stat_dict)
    report_lst = []
    max_ratio = 0
    for table_id in schema_stat_dict:
        schema_count = schema_stat_dict[table_id]['count']
        compress_count = compress_stat_dict[table_id]['count']
        ratio = round(schema_count / compress_count, 2)
        if ratio > max_ratio:
            max_ratio = ratio
        out_item = {'table_id':table_id, 
                    'schema_count':schema_count, 
                    'compress_count':compress_count, 
                    'ratio':ratio}
        report_lst.append(out_item)
    sorted_report_lst = sorted(report_lst, key=lambda x: x['ratio'], reverse=True)
    df_group = group_by_ratio(sorted_report_lst, max_ratio)
    out_group_file = f'./output/{dataset}/schema_compress_ratio_group.csv'
    df_group.to_csv(out_group_file)

    out_file = f'./output/{dataset}/schema_compress_report.jsonl'
    with open(out_file, 'w') as f_o:
        for report_item in sorted_report_lst:
            f_o.write(json.dumps(report_item) + '\n')
    print(f'report is written to {out_file}')
    output_csv(dataset, sorted_report_lst)

def group_by_ratio(report_lst, max_ratio):
    max_int_ratio = int(max_ratio)
    if max_int_ratio < max_ratio:
        max_int_ratio += 1
    
    out_group_dict = {}
    for item in report_lst:
        ratio = item['ratio']
        int_ratio = int(ratio)
        if ratio - int_ratio < 0.5:
            key = f'[{int_ratio}-{int_ratio+0.5})'
        else:
            key = f'[{int_ratio+0.5}-{int_ratio+1})'

        if key not in out_group_dict:
            out_group_dict[key] = []
        ratio_lst = out_group_dict[key]
        ratio_lst.append(int_ratio)
    
    key_lst = list(out_group_dict.keys())
    key_lst.sort()

    out_data = []
    for key in key_lst:
        out_item = [key, len(out_group_dict[key])]
        out_data.append(out_item)
    
    df = pd.DataFrame(out_data, columns=['ratio', 'table count'])
    return df 

def output_csv(dataset, report_lst):
    col_names = ['table_id', 'schema_count', 'compress_count', 'ratio']
    out_data = []
    for item in report_lst:
        out_item = [item['table_id'], item['schema_count'], item['compress_count'], item['ratio']]
        out_data.append(out_item)
    df = pd.DataFrame(out_data, columns=col_names)
    out_file = f'./output/{dataset}/schema_compress_report.csv'
    df.to_csv(out_file)

if __name__ == '__main__':
    main()
