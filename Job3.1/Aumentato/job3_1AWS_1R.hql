DROP TABLE IF EXISTS flights;

-- 1. Creazione della Tabella Esterna puntata su S3
CREATE EXTERNAL TABLE flights (
    carrier STRING,
    origin STRING,
    arr_delay FLOAT,
    cancelled INT,
    month INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://nome_bucket_s3/input/job3.1/' 
TBLPROPERTIES ("skip.header.line.count"="1");

-- IL COMANDO 'LOAD DATA LOCAL INPATH' È STATO RIMOSSO. 
-- Hive leggerà automaticamente tutti i file CSV presenti nella cartella /input/ di S3.

-- 2. Scrittura dell'output direttamente su S3
INSERT OVERWRITE DIRECTORY 's3://nome_bucket_s3/output_hive3_1_1R/'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
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
GROUP BY carrier, origin
ORDER BY carrier, origin;