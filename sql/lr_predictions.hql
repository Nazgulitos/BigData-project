USE team15_projectdb;

DROP TABLE IF EXISTS lr_predictions;

CREATE EXTERNAL TABLE lr_predictions (
    id INT,
    actual FLOAT,
    predicted FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team15/project/hive/warehouse/lr_predictions';