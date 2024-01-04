if [ "$#" -ne 3 ]; then
    echo "Usage: ./metrics.sh <dataset> <strategy> <top>"
    exit
fi
work_dir="$(dirname "$PWD")"
dataset=$1
strategy=$2
top=$3
python ./compute_retr_metrics.py \
    --work_dir ${work_dir} \
    --dataset ${dataset} \
    --strategy ${strategy} \
    --top ${top} \
