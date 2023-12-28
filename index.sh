if [ "$#" -ne 2 ]; then
    echo "Usage: ./encode.sh <dataset> <strategy>"
    exit
fi
work_dir="$(dirname "$PWD")"
dataset=$1
strategy=$2
export PYTHONPATH=${work_dir}/open_table_discovery:${work_dir}/open_table_discovery/relevance
index_src_dir=${work_dir}/open_table_discovery/table2txt/dataset/${dataset}
mkdir -p index_src_dir
if [ ! -d "${index_src_dir}/${strategy}" ]; 
then
    ln -s $PWD/output/${dataset}/${strategy} ${index_src_dir} 
fi
python ./index.py \
    --work_dir ${work_dir} \
    --dataset ${dataset} \
    --strategy ${strategy} \

ln -s $PWD/output/${dataset}/${strategy}/passages.jsonl ${work_dir}/index/on_disk_index_${dataset}_${strategy} 

