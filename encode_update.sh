if [ "$#" -ne 2 ]; then
    echo "Usage: ./encode._update.sh <dataset> <strategy>"
    exit
fi
dataset=$1
strategy=$2
python ./serial_test_encoder.py \
    --dataset ${dataset} \
    --strategy ${strategy} \
