USE team15_projectdb;

DROP TABLE IF EXISTS evaluation_results;

CREATE EXTERNAL TABLE evaluation_results (
    model STRING,
    RMSE FLOAT,
    R2 FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/team15/project/output/evaluation.csv';

-- Query output for charting
INSERT OVERWRITE DIRECTORY 'project/output/chart_evaluation'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM evaluation_results;