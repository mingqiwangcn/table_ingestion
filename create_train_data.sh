if [ "$#" -ne 2 ]; then
    echo "Usage: ./create_train_data.sh <dataset> <strategy>"
    exit
fi
python serial_create_train_data.py \
   --dataset $1 \
   --strategy $2 \
