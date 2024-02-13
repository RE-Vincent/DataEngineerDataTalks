-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `terraform-demo-412901.ny_taxi.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://mage-zoomcamp-rm-01/nyc_taxi_data.parquet']
);

SELECT COUNT(*)
FROM terraform-demo-412901.ny_taxi.external_yellow_tripdata;

SELECT * FROM
terraform-demo-412901.ny_taxi.external_yellow_tripdata
LIMIT 5;

-- Exclude 
SELECT * EXCEPT(tpep_pickup_datetime, tpep_dropoff_datetime, __index_level_0__),
  TIMESTAMP_MILLIS(CAST(tpep_pickup_datetime / 1000000 AS INT64)) AS tpep_pickup_datetime,
  TIMESTAMP_MILLIS(CAST(tpep_dropoff_datetime / 1000000 AS INT64)) AS tpep_dropoff_datetime
FROM (
  SELECT *
  FROM terraform-demo-412901.ny_taxi.external_yellow_tripdata
)
LIMIT 5;

-- Create a view
CREATE OR REPLACE VIEW terraform-demo-412901.ny_taxi.taxi_transformed_view AS
SELECT * EXCEPT(tpep_pickup_datetime, tpep_dropoff_datetime, __index_level_0__),
    TIMESTAMP_MILLIS(CAST(tpep_pickup_datetime / 1000000 AS INT64)) AS tpep_pickup_datetime,
    TIMESTAMP_MILLIS(CAST(tpep_dropoff_datetime / 1000000 AS INT64)) AS tpep_dropoff_datetime
FROM terraform-demo-412901.ny_taxi.external_yellow_tripdata;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE terraform-demo-412901.ny_taxi.yellow_tripdata_non_partitoned AS
SELECT * FROM terraform-demo-412901.ny_taxi.taxi_transformed_view;

SELECT * 
FROM `terraform-demo-412901.ny_taxi.INFORMATION_SCHEMA.TABLES` 
WHERE table_name = 'yellow_tripdata_non_partitoned';

SELECT * 
EXCEPT (__index_level_0__)
FROM terraform-demo-412901.ny_taxi.external_yellow_tripdata
LIMIT 10;


-- Create a partitioned table from view
CREATE OR REPLACE TABLE terraform-demo-412901.ny_taxi.yellow_tripdata_partitoned
PARTITION BY
  DATE(tpep_pickup_datetime)
  AS
SELECT * FROM terraform-demo-412901.ny_taxi.taxi_transformed_view;

SELECT DISTINCT(DATE(tpep_pickup_datetime)) AS partition_date
FROM terraform-demo-412901.ny_taxi.yellow_tripdata_partitoned;

-- Impact of partition
-- Scanning 1.6GB of data
SELECT DISTINCT(VendorID)
FROM terraform-demo-412901.ny_taxi.yellow_tripdata_non_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2021-01-21' AND '2021-01-28';

-- Scanning ~106 MB of DATA
SELECT DISTINCT(VendorID)
FROM terraform-demo-412901.ny_taxi.yellow_tripdata_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2021-01-21' AND '2021-01-28';

-- Let's look into the partitons
SELECT table_name, partition_id, total_rows
FROM `ny_taxi.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitoned'
ORDER BY total_rows DESC;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE terraform-demo-412901.ny_taxi.yellow_tripdata_partitoned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM terraform-demo-412901.ny_taxi.taxi_transformed_view;

-- Query scans 1.1 GB
SELECT count(*) as trips
FROM terraform-demo-412901.ny_taxi.yellow_tripdata_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2021-01-21' AND '2021-01-28'
  AND VendorID=1;

-- Query scans 864.5 MB
SELECT count(*) as trips
FROM terraform-demo-412901.ny_taxi.yellow_tripdata_partitoned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN '2021-01-21' AND '2021-01-28'
  AND VendorID=1;

-- Drop view
DROP VIEW `terraform-demo-412901.ny_taxi.taxi_transformed_view`;


---------TASK 3
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `terraform-demo-412901.ny_taxi.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://mage-zoomcamp-rm-01/green/green_tripdata_2022-*.parquet']
);

SELECT COUNT(*)
FROM terraform-demo-412901.ny_taxi.external_green_tripdata;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE terraform-demo-412901.ny_taxi.green_non_partitoned AS
SELECT * FROM terraform-demo-412901.ny_taxi.external_green_tripdata;

SELECT COUNT(DISTINCT PULocationID)
FROM terraform-demo-412901.ny_taxi.external_green_tripdata;

SELECT COUNT(DISTINCT(PULocationID))
FROM terraform-demo-412901.ny_taxi.green_non_partitoned;

SELECT PULocationID
FROM terraform-demo-412901.ny_taxi.external_green_tripdata;

-- How many records have a fare_amount of 0?
SELECT COUNT(*)
FROM terraform-demo-412901.ny_taxi.external_green_tripdata
WHERE fare_amount = 0;

-- What is the best strategy to make an optimized table in Big Query?
-- Creating a partition and cluster table
CREATE OR REPLACE TABLE terraform-demo-412901.ny_taxi.green_partitoned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID AS
SELECT * FROM terraform-demo-412901.ny_taxi.green_non_partitoned;

-- What's the size of the tables?

SELECT DISTINCT(PULocationID) as locationid
FROM terraform-demo-412901.ny_taxi.green_non_partitoned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

SELECT DISTINCT(PULocationID) as locationid
FROM terraform-demo-412901.ny_taxi.green_partitoned_clustered
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';