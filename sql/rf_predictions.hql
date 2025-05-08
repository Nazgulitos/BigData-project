USE team15_projectdb;

DROP TABLE IF EXISTS rf_predictions;

CREATE EXTERNAL TABLE rf_predictions (
    id INT,
    actual FLOAT,
    predicted FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/team15/project/output/rf_model_predictions.csv';

INSERT OVERWRITE DIRECTORY 'project/output/chart_rf_predictions'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM rf_predictions;