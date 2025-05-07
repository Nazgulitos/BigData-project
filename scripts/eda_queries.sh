#!/bin/bash

password=$(head -n 1 secrets/.hive.pass)

echo "Starting EDA queries..."

echo "Q1..."
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team15 -p "$password" --hiveconf hive.resultset.use.unique.column.names=false -f sql/q1_overall_cancellation_rate.hql > output/q1_overall_cancellation_rate.csv 2>/dev/null
if [ $? -ne 0 ]; then echo "Warn: Q1 failed."; fi

echo "Q2..."
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team15 -p "$password" --hiveconf hive.resultset.use.unique.column.names=false -f sql/q2_cancellations_by_month.hql > output/q2_cancellations_by_month.csv 2>/dev/null
if [ $? -ne 0 ]; then echo "Warn: Q2 failed."; fi

echo "Q3..."
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team15 -p "$password" --hiveconf hive.resultset.use.unique.column.names=false -f sql/q3_top_origin_cancellations.hql > output/q3_top_origin_cancellations.csv 2>/dev/null
if [ $? -ne 0 ]; then echo "Warn: Q3 failed."; fi

echo "Q4..."
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team15 -p "$password" --hiveconf hive.resultset.use.unique.column.names=false -f sql/q4_top_cancellation_reasons.hql > output/q4_top_cancellation_reasons.csv 2>/dev/null
if [ $? -ne 0 ]; then echo "Warn: Q4 failed."; fi

echo "Q5..."
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team15 -p "$password" --hiveconf hive.resultset.use.unique.column.names=false -f sql/q5_cancellation_by_dow.hql > output/q5_cancellation_by_dow.csv 2>/dev/null
if [ $? -ne 0 ]; then echo "Warn: Q5 failed."; fi

echo "Q6..."
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team15 -p "$password" --hiveconf hive.resultset.use.unique.column.names=false -f sql/q6_cancellation_by_depdelay.hql > output/q6_cancellation_by_depdelay.csv 2>/dev/null
if [ $? -ne 0 ]; then echo "Warn: Q6 failed."; fi

echo "EDA queries finished."