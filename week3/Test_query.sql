Create or replace table taxi_ny_2024.yellow_tripdata as
select * from `taxi_ny_2024.yellow_Jan`
UNION ALL select * from `taxi_ny_2024.yellow_Feb`
UNION ALL select * from `taxi_ny_2024.yellow_Mar`
UNION ALL select * from `taxi_ny_2024.yellow_Apr`
UNION ALL select * from `taxi_ny_2024.yellow_May`
UNION ALL select * from `taxi_ny_2024.yellow_Jun`
;

select count(*) from `taxi_ny_2024.yellow_tripdata`;
--  it does not scan the actual data when performing COUNT(*) on partitioned or clustered tables. Instead, it retrieves the row count from table metadata, which incurs no data processing costs.

select count(distinct PULocationID) from `taxi_ny_2024.yellow_tripdata`;

select count(*) from `taxi_ny_2024.yellow_tripdata`
where fare_amount = 0;

Create or replace table taxi_ny_2024.new_patitioned
partition by DATE(tpep_dropoff_datetime) CLUSTER BY VendorID 
AS
SELECT * FROM `taxi_ny_2024.yellow_tripdata`;

select distinct VendorID from `taxi_ny_2024.yellow_tripdata`
where tpep_dropoff_datetime between '2024-03-01' and '2024-03-15';

select distinct VendorID from `taxi_ny_2024.new_patitioned`
where tpep_dropoff_datetime between '2024-03-01' and '2024-03-15';

