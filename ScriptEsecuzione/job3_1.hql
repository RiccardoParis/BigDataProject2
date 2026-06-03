-- Creiamo il database per sicurezza
CREATE DATABASE IF NOT EXISTS job3_1_db;
USE job3_1_db;

DROP TABLE IF EXISTS flights;

-- Utilizziamo una EXTERNAL TABLE che punta direttamente alla cartella di input passata.
-- Questo è molto più sicuro del "LOAD DATA" perché non sposta/cancella il file originale su HDFS.
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
LOCATION '${INPUT_PATH}'
TBLPROPERTIES ("skip.header.line.count"="1");

-- La query finale inserita in un INSERT OVERWRITE per salvare il risultato nell'output specificato
INSERT OVERWRITE DIRECTORY '${OUTPUT_PATH}' 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
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