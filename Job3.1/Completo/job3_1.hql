DROP TABLE IF EXISTS flights;

CREATE TABLE flights (
    carrier STRING,
    origin STRING,
    arr_delay FLOAT,
    cancelled INT,
    month INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH 'dataset_job_3_1_completo.csv' OVERWRITE INTO TABLE flights;

SELECT 
    carrier, 
    origin, 
    COUNT(*) as total_flights,
    MIN(arr_delay) as min_delay,
    MAX(arr_delay) as max_delay,
    AVG(arr_delay) as avg_delay,
    (SUM(cancelled) / COUNT(*)) * 100 as cancellation_rate,
    concat_ws('-', collect_set(cast(month as string))) as months_active
FROM flights
GROUP BY carrier, origin;