#!/bin/bash

password=$(head -n 1 secrets/.hive.pass)

echo "Starting PDA queries..."

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team15 -p "$password" -f sql/evaluation_results.hql
echo "model,RMSE,R2" > output/evaluation.csv
hdfs dfs -cat project/output/chart_evaluation/* >> output/evaluation.csv 2>/dev/null

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team15 -p "$password" -f sql/lr_predictions.hql
echo "id,actual,predicted" > output/lr_predictions.csv
hdfs dfs -cat project/output/chart_lr_predictions/* >> output/lr_predictions.csv 2>/dev/null

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team15 -p "$password" -f sql/rf_predictions.hql
echo "id,actual,predicted" > output/rf_predictions.csv
hdfs dfs -cat project/output/chart_rf_predictions/* >> output/rf_predictions.csv 2>/dev/null

echo "PDA queries finished."