if [ "$#" -ne 1 ]; then
    echo "Usage: ./gen_questions.sh <dataset>"
    exit
fi
export PYTHONPATH=~/code/solo_work/table_ingestion
python generate.py \
    --work_dir ~/code/solo_work \
    --dataset $1 \
    