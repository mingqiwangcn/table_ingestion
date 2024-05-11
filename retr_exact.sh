if [ "$#" -ne 2 ]; then
    echo "Usage: ./retr_exact.sh <dataset> <strategy>"
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
    --query_tag test \
    --index_type exact\
    --use_student 0 \
