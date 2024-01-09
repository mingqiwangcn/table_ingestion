if [ "$#" -ne 2 ]; then
    echo "Usage: ./test.sh <dataset> <strategy>"
    exit
fi
python test.py \
   --dataset $1 \
   --strategy $2 \
   --debug 0
