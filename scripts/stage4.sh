#!/bin/bash

password=$(head -n 1 secrets/.hive.pass)

echo "Starting PDA queries..."

# === LINEAR REGRESSION ===
echo "Preparing LR predictions..."
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team15 -p "$password" -f sql/lr_predictions.hql

hdfs dfs -rm -r -f /user/team15/project/hive/warehouse/lr_predictions
hdfs dfs -mkdir -p /user/team15/project/hive/warehouse/lr_predictions
hdfs dfs -cp /user/team15/project/output/lr_model_predictions.csv /user/team15/project/hive/warehouse/lr_predictions/

# === RANDOM FOREST ===
echo "Preparing RF predictions..."
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team15 -p "$password" -f sql/rf_predictions.hql

hdfs dfs -rm -r -f /user/team15/project/hive/warehouse/rf_predictions
hdfs dfs -mkdir -p /user/team15/project/hive/warehouse/rf_predictions
hdfs dfs -cp /user/team15/project/output/rf_model_predictions.csv /user/team15/project/hive/warehouse/rf_predictions/

# === EVALUATION RESULTS ===
echo "Preparing evaluation results..."
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team15 -p "$password" -f sql/evaluation.hql

hdfs dfs -rm -r -f /user/team15/project/hive/warehouse/evaluation
hdfs dfs -mkdir -p /user/team15/project/hive/warehouse/evaluation
hdfs dfs -cp /user/team15/project/output/evaluation.csv /user/team15/project/hive/warehouse/evaluation/

echo "PDA queries finished."