if [ "$#" -ne 2 ]; then
    echo "Usage: ./gen_questions.sh <dataset> <strategy>"
    exit
fi
export PYTHONPATH=~/code/table_project/table_ingestion
python generate.py \
    --dataset $1 \
    --strategy $2
