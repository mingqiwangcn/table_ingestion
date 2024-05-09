if [ "$#" -ne 2 ]; then
    echo "Usage: ./create_train_passages.sh <dataset> <strategy>"
    exit
fi
python create_train_passages.py \
   --dataset $1 \
   --strategy $2 \
