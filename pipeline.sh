if [ "$#" -ne 2 ]; then
    echo "Usage: ./pipeline.sh <dataset> <strategy>"
    exit
fi
dataset=$1
strategy=$2
rm ./output/${dataset}/${strategy}/passages.jsonl
./serialize.sh ${dataset} ${strategy}
rm -rf ./output/${dataset}/${strategy}/emb/
./encode.sh ${dataset} ${strategy}

rm -rf ../data/${dataset}/query/test/${strategy}/exact
./retr_exact.sh ${dataset} ${strategy}

./metrics.sh ${dataset} ${strategy} 10
