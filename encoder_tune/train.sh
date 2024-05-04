if [ "$#" -ne 2 ]; then
    echo "Usage: ./train.sh <dataset> <strategy>"
    exit
fi
python serial_train.py \
   --dataset $1 \
   --strategy $2 \
