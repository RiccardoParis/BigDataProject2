from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Job3.2_SparkSQL") \
        .getOrCreate()

    # 1. Lettura Dati
    df = spark.read.csv("s3://nome_bucket_s3/input/job3.2/dataset_job_3_2_aumentato.csv", header=True, inferSchema=True)

    # 2. Logica Fasce (Tier)
    df_tiers = df.withColumn("tier", 
        F.when(F.col("cancelled") == 1.0, "CANCELLED")
         .when((F.col("dep_delay") > 0) & (F.col("dep_delay") < 15), "LOW")
         .when((F.col("dep_delay") >= 15) & (F.col("dep_delay") <= 60), "MEDIUM")
         .when(F.col("dep_delay") > 60, "HIGH")
         .otherwise("UNKNOWN")
    ).filter(F.col("tier") != "CANCELLED")

    # Aggregazione Fasce
    stats = df_tiers.groupBy("origin", "month").agg(
        F.sum(F.when(F.col("tier") == "LOW", 1).otherwise(0)).alias("count_low"),
        F.round(F.avg(F.when(F.col("tier") == "LOW", F.col("dep_delay"))), 2).alias("avg_dep_low"),
        F.round(F.avg(F.when(F.col("tier") == "LOW", F.col("arr_delay"))), 2).alias("avg_arr_low"),
        F.sum(F.when(F.col("tier") == "MEDIUM", 1).otherwise(0)).alias("count_medium"),
        F.round(F.avg(F.when(F.col("tier") == "MEDIUM", F.col("dep_delay"))), 2).alias("avg_dep_medium"),
        F.round(F.avg(F.when(F.col("tier") == "MEDIUM", F.col("arr_delay"))), 2).alias("avg_arr_medium"),
        F.sum(F.when(F.col("tier") == "HIGH", 1).otherwise(0)).alias("count_high"),
        F.round(F.avg(F.when(F.col("tier") == "HIGH", F.col("dep_delay"))), 2).alias("avg_dep_high"),
        F.round(F.avg(F.when(F.col("tier") == "HIGH", F.col("arr_delay"))), 2).alias("avg_arr_high")
    )

    # 3. Logica Cause (Top 3)
    cause_cols = [
        F.when(F.col("carrier_delay") > 0, "Carrier_Delay").otherwise(None),
        F.when(F.col("weather_delay") > 0, "Weather_Delay").otherwise(None),
        F.when(F.col("nas_delay") > 0, "NAS_Delay").otherwise(None),
        F.when(F.col("security_delay") > 0, "Security_Delay").otherwise(None),
        F.when(F.col("late_aircraft_delay") > 0, "Late_Aircraft_Delay").otherwise(None)
    ]
    
    # Esplosione delle cause
    df_causes = df.withColumn("cause", F.explode(F.array(*cause_cols))) \
                  .filter(F.col("cause").isNotNull()) \
                  .groupBy("origin", "month", "cause").count()

    # Ranking Top 3
    window_spec = Window.partitionBy("origin", "month").orderBy(F.col("count").desc())
    top3 = df_causes.withColumn("rnk", F.row_number().over(window_spec)) \
                    .filter(F.col("rnk") <= 3) \
                    .groupBy("origin", "month") \
                    .agg(F.collect_list(F.concat(F.col("cause"), F.lit("("), F.col("count"), F.lit(")"))).alias("top3_list"))

    # 4. Join finale e Formattazione
    final_df = stats.join(top3, ["origin", "month"], "left") \
        .select(
            "origin", "month",
            F.concat(F.lit("Low:"), F.coalesce(F.col("count_low"), F.lit(0)), F.lit(" voli (DepAvg:"), F.coalesce(F.col("avg_dep_low"), F.lit(0.0)), F.lit(", ArrAvg:"), F.coalesce(F.col("avg_arr_low"), F.lit(0.0)), F.lit(")")).alias("low_str"),
            F.concat(F.lit("Med:"), F.coalesce(F.col("count_medium"), F.lit(0)), F.lit(" voli (DepAvg:"), F.coalesce(F.col("avg_dep_medium"), F.lit(0.0)), F.lit(", ArrAvg:"), F.coalesce(F.col("avg_arr_medium"), F.lit(0.0)), F.lit(")")).alias("med_str"),
            F.concat(F.lit("High:"), F.coalesce(F.col("count_high"), F.lit(0)), F.lit(" voli (DepAvg:"), F.coalesce(F.col("avg_dep_high"), F.lit(0.0)), F.lit(", ArrAvg:"), F.coalesce(F.col("avg_arr_high"), F.lit(0.0)), F.lit(")")).alias("high_str"),
            F.concat(F.lit("Top3_Cause:["), F.coalesce(F.concat_ws(", ", F.col("top3_list")), F.lit("Nessuna_Causa_Rilevata")), F.lit("]")).alias("cause_str")
        ) \
        .orderBy("origin", "month")

    # Scrittura (usiamo coalesce(1) se vuoi un unico file di output, ma occhio alla memoria!)
    final_df.rdd.map(lambda row: "\t".join([str(c) for c in row])).saveAsTextFile("s3://nome_bucket_s3/output_sparksql3_2")