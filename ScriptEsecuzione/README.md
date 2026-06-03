#Lo script run_job.sh permette l'esecuzione di qualunque script passando come parametri l'input l'output e lo script da #eseguire, questo è un esempio di come eseguirli sul job3.1 con il dataset ridotto. Possono essere usati per tutti gli #esperimenti. Gli script presenti in questa cartella sono stati adeguati a ricevere input e output da riga du comando, mentre #MapReduce vanno bene i file presenti nelle cartelle Job3.1/Completo e Job3.2/Completo
#Sintassi dello script di orchestrazione:
```bash
./run_job.sh <tecnologia> <input_path> <output_path> <script_files>

#MapReduce
./run_job.sh mr /user/hadoop/job3.1/input_ridotto/dataset_job_3_1_ridotto.csv /user/hadoop/job3.1/output_mr_param mapper.py,reducer.py

#Hive
./run_job.sh hive /riccardo/hadoop/job3.1/input_ridotto/ /riccardo/hadoop/job3.1/output_hive_param job3_1.hql

#Spark
./run_job.sh spark /riccardo/hadoop/job3.1/input_ridotto/dataset_job_3_1_ridotto.csv /riccardo/hadoop/job3.1/output_spark_param job3_1_spark.py

#SparkSQL
./run_job.sh sparksql /riccardo/hadoop/job3.1/input_ridotto/dataset_job_3_1_ridotto.csv /riccardo/hadoop/job3.1/output_sparksql_param job3_1_sparksql.py
