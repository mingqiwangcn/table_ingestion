export PYTHONPATH=/home/cc/code/table_project/solo/relevance
python tune_student.py \
--do_train \
--lr 1e-4 \
--optim adamw \
--scheduler linear \
--model_path /home/cc/code/table_project/models/student_tqa_retriever_step_29500 \
--teacher_model_path /home/cc/code/table_project/models/student_tqa_retriever_step_29500 \
--train_data /home/cc/code/table_project/table_ingestion/output/nyc_open_10K/schema_cell_coding/samples/train/train_cpr.jsonl \
--train_teacher_data /home/cc/code/table_project/table_ingestion/output/nyc_open_10K/schema_cell_coding/samples/train/train_base.jsonl \
--student_teacher_map /home/cc/code/table_project/table_ingestion/output/nyc_open_10K/schema_cell_coding/samples/train/train_cpr_base_map.jsonl \
--eval_data /home/cc/code/table_project/table_ingestion/output/nyc_open_10K/schema_cell_coding/samples/train/dev_cpr.jsonl \
--per_gpu_batch_size 10 \
--total_steps 20000 \
--scheduler_steps 30000 \
--teacher_precompute_file /home/cc/code/table_project/table_ingestion/output/nyc_open_10K/schema_cell_coding/samples/train/teacher_precom_emb/teacher_precompute_triviaqa.pl \
--distill_temperature 3 \
--distill_weight 0.5 \
--dropout 0.1 \