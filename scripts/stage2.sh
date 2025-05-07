#!/bin/bash

# hdfs dfs -rm -r -skipTrash /user/team15/project/hive/warehouse/flight_optimized 2>/dev/null
# hdfs dfs -rm -r -skipTrash /user/team15/project/hive/warehouse/airport_optimized 2>/dev/null
# hdfs dfs -rm -r -skipTrash /user/team15/project/hive/warehouse/cancellationreason_optimized 2>/dev/null

# hdfs dfs -mkdir -p project/warehouse/avsc

# hdfs dfs -put output/*.avsc project/warehouse/avsc

password=$(head -n 1 secrets/.hive.pass)

# beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team15 -p $password -f sql/db.hql

# beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team15 -p $password -f sql/db.hql > output/hive_results.txt

# beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team15 -p "$password" -f sql/optimization.hql

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team15 -p $password -f scripts/eda_queries.sh
