#!/usr/bin/env python3
from pyspark.sql import SparkSession

# 1. Inizializzazione della Spark Session
spark = SparkSession.builder \
    .appName("Job3.1_SparkSQL") \
    .getOrCreate()

# 2. Lettura del dataset (usiamo il percorso HDFS assoluto per evitare errori)
df = spark.read.csv("/user/hadoop/job3.1/input/dataset_job_3_1_completo.csv", header=True, inferSchema=True)

# 3. Creazione della Vista Temporanea
# Questo permette a Spark di trattare il DataFrame come una tabella SQL chiamata 'voli'
df.createOrReplaceTempView("voli")

# 4. Query SparkSQL
# Nota: usiamo array_join e array_sort per replicare l'ordinamento e la concatenazione dei mesi
query = """
SELECT 
    op_unique_carrier, 
    origin, 
    COUNT(*) AS total_flights, 
    MIN(arr_delay) AS min_delay, 
    MAX(arr_delay) AS max_delay, 
    ROUND(AVG(arr_delay), 2) AS avg_delay, 
    CONCAT(ROUND((SUM(cancelled) / COUNT(*)) * 100, 2), '%') AS cancellation_rate,
    array_join(array_sort(collect_set(CAST(month AS STRING))), '-') AS months_active
FROM voli
GROUP BY op_unique_carrier, origin
"""

# Esecuzione della query
result = spark.sql(query)

# 5. Scrittura dell'output
result.coalesce(1).write.csv("/user/hadoop/job3.1/output_sparksql_job3_1", sep='\t', header=False)

spark.stop()