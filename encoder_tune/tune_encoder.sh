if [ "$#" -ne 2 ]; then
    echo "Usage: ./train.sh <dataset> <strategy>"
    exit
fi
export PYTHONPATH=~/code/table_project/table_ingestion
python tune_encoder.py \
   --dataset $1 \
   --strategy $2 \
