USE team15_projectdb;

DROP TABLE IF EXISTS evaluation_results;

CREATE EXTERNAL TABLE evaluation_results (
    model STRING,
    RMSE FLOAT,
    R2 FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team15/project/hive/warehouse/evaluation';