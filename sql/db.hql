DROP DATABASE IF EXISTS team15_projectdb CASCADE;

CREATE DATABASE team15_projectdb LOCATION "project/hive/warehouse";
USE team15_projectdb;

CREATE EXTERNAL TABLE airport STORED AS AVRO LOCATION 'project/warehouse' TBLPROPERTIES ('avro.schema.url'='project/warehouse/airport.avsc');
CREATE EXTERNAL TABLE flight STORED AS AVRO LOCATION 'project/warehouse' TBLPROPERTIES ('avro.schema.url'='project/warehouse/flight.avsc');
CREATE EXTERNAL TABLE cancellationreason STORED AS AVRO LOCATION 'project/warehouse' TBLPROPERTIES ('avro.schema.url'='project/warehouse/cancellationreason.avsc');

select * from cancellationreason limit 5;
select * from airport limit 5;
select * from flight limit 5;
