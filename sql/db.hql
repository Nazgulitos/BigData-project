DROP TABLE IF EXISTS flight CASCADE;
DROP TABLE IF EXISTS airport CASCADE;
DROP TABLE IF EXISTS cancellationreason CASCADE;

CREATE EXTERNAL TABLE airport STORED AS AVRO LOCATION 'project/warehouse' TBLPROPERTIES ('avro.schema.url'='project/warehouse/airport.avsc');
CREATE EXTERNAL TABLE flight STORED AS AVRO LOCATION 'project/warehouse' TBLPROPERTIES ('avro.schema.url'='project/warehouse/flight.avsc');
CREATE EXTERNAL TABLE cancellationreason STORED AS AVRO LOCATION 'project/warehouse' TBLPROPERTIES ('avro.schema.url'='project/warehouse/cancellationreason.avsc');

select * from cancellationreason limit 5;
select * from airport limit 5;
select * from flight limit 5;
