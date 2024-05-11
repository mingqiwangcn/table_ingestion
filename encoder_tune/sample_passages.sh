if [ "$#" -ne 2 ]; then
    echo "Usage: ./create_train_passages.sh <dataset> <strategy>"
    exit
fi
export PYTHONPATH=~/code/table_project/table_ingestion
python create_train_passages.py \
   --dataset $1 \
   --strategy $2 \
   --sample_size 5000 \
