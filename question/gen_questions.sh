if [ "$#" -ne 1 ]; then
    echo "Usage: ./gen_questions.sh <dataset>"
    exit
fi

export PYTHONPATH=~/code/solo_work

python generate.py \
    --dataset $1
