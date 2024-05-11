if [ "$#" -ne 2 ]; then
    echo "Usage: ./sample_passages.sh <dataset> <strategy>"
    exit
fi
export PYTHONPATH=~/code/table_project/table_ingestion
python sample_passages.py \
   --dataset $1 \
   --strategy $2 \
   --sample_size 5000 \
