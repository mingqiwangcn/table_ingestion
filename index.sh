work_dir="$(dirname "$PWD")"
export PYTHONPATH=${work_dir}/open_table_discovery:${work_dir}/open_table_discovery/relevance
python ./index.py \
--work_dir ${work_dir} \
