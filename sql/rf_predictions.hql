USE team15_projectdb;

DROP TABLE IF EXISTS rf_predictions;

CREATE EXTERNAL TABLE rf_predictions (
    actual FLOAT,
    predicted FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/team15/project/hive/warehouse/rf_predictions';
