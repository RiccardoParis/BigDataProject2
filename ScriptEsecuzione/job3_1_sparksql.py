#!/usr/bin/env python3
import sys
from pyspark.sql import SparkSession

if len(sys.argv) != 3:
    print("Uso: job3_1_sparksql.py <input_path> <output_path>")
    sys.exit(-1)

input_path = sys.argv[1]
output_path = sys.argv[2]

spark = SparkSession.builder.appName("Job3.1_SparkSQL").getOrCreate()

# SOSTITUISCI il percorso fisso con input_path
df = spark.read.csv(input_path, header=True, inferSchema=True)

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
result.write.csv(output_path, sep="\t", header=False)

spark.stop()