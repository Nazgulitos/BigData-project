#!/bin/bash

password=$(head -n 1 secrets/.hive.pass)

echo "Starting EDA queries..."

echo "Q1: Overall Cancellation Rate..."
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team15 -p "$password" --hiveconf hive.resultset.use.unique.column.names=false -f sql/chart1.hql > output/chart1.csv 2>/dev/null
if [ $? -ne 0 ]; then echo "Warn: Q1 failed."; fi

echo "Q2: Cancellations by Month..."
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team15 -p "$password" --hiveconf hive.resultset.use.unique.column.names=false -f sql/chart2.hql > output/chart2.csv 2>/dev/null
if [ $? -ne 0 ]; then echo "Warn: Q2 failed."; fi

echo "Q3: Top Origin Airport Cancellations..."
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team15 -p "$password" --hiveconf hive.resultset.use.unique.column.names=false -f sql/chart3.hql > output/chart3.csv 2>/dev/null
if [ $? -ne 0 ]; then echo "Warn: Q3 failed."; fi

echo "Q4: Top Cancellation Reasons..."
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team15 -p "$password" --hiveconf hive.resultset.use.unique.column.names=false -f sql/chart4.hql > output/chart4.csv 2>/dev/null
if [ $? -ne 0 ]; then echo "Warn: Q4 failed."; fi

echo "Q5: Cancellation Rate by Day of Week..."
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team15 -p "$password" --hiveconf hive.resultset.use.unique.column.names=false -f sql/chart5.hql > output/chart5.csv 2>/dev/null
if [ $? -ne 0 ]; then echo "Warn: Q5 failed."; fi

echo "Q6: Cancellation Rate by Departure Delay..."
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team15 -p "$password" --hiveconf hive.resultset.use.unique.column.names=false -f sql/chart6.hql > output/chart6.csv 2>/dev/null
if [ $? -ne 0 ]; then echo "Warn: Q6 failed."; fi

echo "EDA queries finished."