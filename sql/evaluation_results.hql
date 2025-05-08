USE team15_projectdb;

DROP TABLE IF EXISTS evaluation_results;

CREATE EXTERNAL TABLE evaluation_results (
    model STRING,
    auc FLOAT,
    accuracy FLOAT,
    `precision` FLOAT,
    recall FLOAT,
    f1_score FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team15/project/output/evaluation.csv';