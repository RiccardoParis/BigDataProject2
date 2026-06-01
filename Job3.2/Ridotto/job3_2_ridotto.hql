CREATE DATABASE IF NOT EXISTS job3_2_db;
USE job3_2_db;

-- FORZIAMO LA CANCELLAZIONE DELLA VECCHIA TABELLA
DROP TABLE IF EXISTS voli_job3_2;

-- 1. TABELLA PRINCIPALE (se non esiste)
CREATE EXTERNAL TABLE IF NOT EXISTS voli_job3_2 (
    origin STRING, month INT, dep_delay DOUBLE, arr_delay DOUBLE,
    cancelled DOUBLE, cancellation_code STRING, carrier_delay DOUBLE,
    weather_delay DOUBLE, nas_delay DOUBLE, security_delay DOUBLE, late_aircraft_delay DOUBLE
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/hadoop/job3.2/input_ridotto/' TBLPROPERTIES ("skip.header.line.count"="1");

-- =========================================================
-- STEP 1: Calcolo Fasce. Salva su disco e libera la RAM.
-- =========================================================
DROP TABLE IF EXISTS job3_2_fasce;
CREATE TABLE job3_2_fasce AS
SELECT origin, month,
    SUM(IF(cancelled != 1.0 AND dep_delay > 0 AND dep_delay < 15, 1, 0)) as count_low,
    ROUND(AVG(IF(cancelled != 1.0 AND dep_delay > 0 AND dep_delay < 15, dep_delay, NULL)), 2) as avg_dep_low,
    ROUND(AVG(IF(cancelled != 1.0 AND dep_delay > 0 AND dep_delay < 15, arr_delay, NULL)), 2) as avg_arr_low,
    SUM(IF(cancelled != 1.0 AND dep_delay >= 15 AND dep_delay <= 60, 1, 0)) as count_medium,
    ROUND(AVG(IF(cancelled != 1.0 AND dep_delay >= 15 AND dep_delay <= 60, dep_delay, NULL)), 2) as avg_dep_medium,
    ROUND(AVG(IF(cancelled != 1.0 AND dep_delay >= 15 AND dep_delay <= 60, arr_delay, NULL)), 2) as avg_arr_medium,
    SUM(IF(cancelled != 1.0 AND dep_delay > 60, 1, 0)) as count_high,
    ROUND(AVG(IF(cancelled != 1.0 AND dep_delay > 60, dep_delay, NULL)), 2) as avg_dep_high,
    ROUND(AVG(IF(cancelled != 1.0 AND dep_delay > 60, arr_delay, NULL)), 2) as avg_arr_high
FROM voli_job3_2 GROUP BY origin, month;

-- =========================================================
-- STEP 2: Isoliamo le cause e le salviamo su disco.
-- =========================================================
DROP TABLE IF EXISTS job3_2_cause_raw;
CREATE TABLE job3_2_cause_raw AS
SELECT origin, month, CONCAT('Cancellazione_', cancellation_code) as cause FROM voli_job3_2 WHERE cancelled = 1.0 AND cancellation_code != 'N' AND cancellation_code IS NOT NULL AND cancellation_code != ''
UNION ALL SELECT origin, month, 'Carrier_Delay' as cause FROM voli_job3_2 WHERE carrier_delay > 0
UNION ALL SELECT origin, month, 'Weather_Delay' as cause FROM voli_job3_2 WHERE weather_delay > 0
UNION ALL SELECT origin, month, 'NAS_Delay' as cause FROM voli_job3_2 WHERE nas_delay > 0
UNION ALL SELECT origin, month, 'Security_Delay' as cause FROM voli_job3_2 WHERE security_delay > 0
UNION ALL SELECT origin, month, 'Late_Aircraft_Delay' as cause FROM voli_job3_2 WHERE late_aircraft_delay > 0;

-- =========================================================
-- STEP 3: Calcoliamo la Top 3 partendo dalla tabella precedente
-- =========================================================
DROP TABLE IF EXISTS job3_2_top3;
CREATE TABLE job3_2_top3 AS
SELECT origin, month, CONCAT_WS(', ', collect_list(CONCAT(cause, '(', CAST(cause_count AS STRING), ')'))) as top3_str
FROM (
    SELECT origin, month, cause, cause_count,
           ROW_NUMBER() OVER(PARTITION BY origin, month ORDER BY cause_count DESC) as rnk
    FROM (
        SELECT origin, month, cause, COUNT(*) as cause_count
        FROM job3_2_cause_raw
        GROUP BY origin, month, cause
    ) counts
) ranked
WHERE rnk <= 3 GROUP BY origin, month;

-- =========================================================
-- STEP 4: Uniamo le tabelle e scriviamo l'output finale
-- =========================================================
INSERT OVERWRITE DIRECTORY '/user/hadoop/job3.2/output_hive' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT 
    f.origin, f.month,
    CONCAT('Low:', COALESCE(f.count_low, 0), ' voli (DepAvg:', COALESCE(f.avg_dep_low, 0.0), ', ArrAvg:', COALESCE(f.avg_arr_low, 0.0), ')') as low_str,
    CONCAT('Med:', COALESCE(f.count_medium, 0), ' voli (DepAvg:', COALESCE(f.avg_dep_medium, 0.0), ', ArrAvg:', COALESCE(f.avg_arr_medium, 0.0), ')') as med_str,
    CONCAT('High:', COALESCE(f.count_high, 0), ' voli (DepAvg:', COALESCE(f.avg_dep_high, 0.0), ', ArrAvg:', COALESCE(f.avg_arr_high, 0.0), ')') as high_str,
    CONCAT('Top3_Cause:[', COALESCE(c.top3_str, 'Nessuna_Causa_Rilevata'), ']') as cause_str
FROM job3_2_fasce f
LEFT JOIN job3_2_top3 c ON f.origin = c.origin AND f.month = c.month
ORDER BY f.origin, f.month;