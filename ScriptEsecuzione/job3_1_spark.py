#!/usr/bin/env python3
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, min, max, avg, sum, round, concat, lit, collect_set, concat_ws, sort_array

# Controlla che vengano passati i parametri
if len(sys.argv) != 3:
    print("Uso: job3_1_spark.py <input_path> <output_path>")
    sys.exit(-1)

input_path = sys.argv[1]
output_path = sys.argv[2]

spark = SparkSession.builder.appName("Job3.1_Spark").getOrCreate()

# SOSTITUISCI il percorso fisso con la variabile input_path
df = spark.read.csv(input_path, header=True, inferSchema=True)

# 3. Trasformazione e Aggregazione
result = df.groupBy("op_unique_carrier", "origin").agg(
    count("*").alias("total_flights"),
    min("arr_delay").alias("min_delay"),
    max("arr_delay").alias("max_delay"),
    
    # L'arrotondamento per la media è opzionale, ma rende l'output più pulito
    round(avg("arr_delay"), 2).alias("avg_delay"),
    
    # Calcolo del tasso di cancellazione identico al tuo MapReduce
    concat(round((sum("cancelled") / count("*")) * 100, 2), lit("%")).alias("cancellation_rate"),
    
    # Cast a stringa, raggruppamento in set unico, ordinamento e concatenazione con '-'
    concat_ws("-", sort_array(collect_set(col("month").cast("string")))).alias("months_active")
)

# 4. Scrittura dell'output
# coalesce(1) forza Spark a scrivere tutto in un unico file (come 1 singolo Reducer)
# sep='\t' garantisce che i campi siano separati da tabulazione come nel tuo test Python
result.write.csv(output_path, sep="\t", header=False)

spark.stop()