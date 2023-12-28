if [ "$#" -ne 2 ]; then
    echo "Usage: ./metrics.sh <dataset> <strategy>"
    exit
fi
work_dir="$(dirname "$PWD")"
dataset=$1
strategy=$2
python ./compute_retr_metrics.py \
    --work_dir ${work_dir} \
    --dataset ${dataset} \
    --strategy ${strategy} \
