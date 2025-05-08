USE team15_projectdb;

DROP TABLE IF EXISTS lr_predictions;

CREATE EXTERNAL TABLE lr_predictions (
    id INT,
    actual FLOAT,
    predicted FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/team15/project/output/lr_model_predictions.csv';

INSERT OVERWRITE DIRECTORY 'project/output/chart_lr_predictions'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM lr_predictions;