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
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/team15/project/output/evaluation.csv';