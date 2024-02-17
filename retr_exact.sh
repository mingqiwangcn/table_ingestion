if [ "$#" -ne 3 ]; then
    echo "Usage: ./retr.sh <dataset> <strategy>"
    exit
fi
work_dir="$(dirname "$PWD")"
dataset=$1
strategy=$2
export PYTHONPATH=${work_dir}/open_table_discovery:${work_dir}/open_table_discovery/relevance
python retrieve.py \
    --work_dir ${work_dir} \
    --dataset ${dataset} \
    --strategy ${strategy} \
