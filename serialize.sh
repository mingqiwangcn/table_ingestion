if [ "$#" -ne 2 ]; then
    echo "Usage: ./serialize.sh <dataset> <strategy>"
    exit
fi
python serialize.py \
   --dataset $1 \
   --strategy $2 \
   --debug 0
