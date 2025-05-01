#!/bin/bash
hdfs dfs -mkdir -p project/warehouse/avsc

hdfs dfs -put output/*.avsc project/warehouse/avsc

password=$(head -n 1 secrets/.hive.pass)

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team15 -p $password -f sql/db.hql

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team15 -p $password -f sql/db.hql > output/hive_results.txt

