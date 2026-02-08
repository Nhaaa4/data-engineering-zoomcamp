CREATE OR REPLACE EXTERNAL TABLE `bigquery-learning-1770549505.nytaxi.yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://bigquery-learning-yellow-taxi-data/yellow_tripdata_2024-*.parquet']
);

SELECT COUNT(*) FROM `bigquery-learning-1770549505.nytaxi.yellow_tripdata`;

SELECT COUNT(DISTINCT(PULocationID)) FROM `bigquery-learning-1770549505.nytaxi.yellow_tripdata`;

SELECT COUNT(fare_amount) FROM `bigquery-learning-1770549505.nytaxi.yellow_tripdata`
WHERE fare_amount = 0;

CREATE OR REPLACE TABLE `bigquery-learning-1770549505.nytaxi.yellow_nonpartitioned_tripdata`
AS SELECT * FROM `bigquery-learning-1770549505.nytaxi.yellow_tripdata`;

CREATE OR REPLACE TABLE `bigquery-learning-1770549505.nytaxi.yellow_partitioned_tripdata`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS (
  SELECT * FROM `bigquery-learning-1770549505.nytaxi.yellow_tripdata`
);

SELECT COUNT(VendorID) FROM  `bigquery-learning-1770549505.nytaxi.yellow_nonpartitioned_tripdata`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';


SELECT COUNT(VendorID) FROM `bigquery-learning-1770549505.nytaxi.yellow_partitioned_tripdata`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
