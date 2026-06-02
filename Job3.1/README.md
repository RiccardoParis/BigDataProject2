#Comandi per riprodurre gli esperimenti eseguiti rigurado il job 3.1. Con wsl sono stati eseguiti gli esperimenti riguardanti il dataset ridotto e completo, di cui vi sono 2 versioni degli script dove cambia solo l'input.
#Invece sull'ambiente cluster sono stati eseguiti gli esperimenti con il dataset aumentato. Sono presenti 2 versioni degli script, di cui uno forza l'esecuzione con un singolo Reducer(1R).

#Esecuzioni su macchina locale attraverso l'utilizzo di wsl
#Nell'esecuzione MapReduce e Hive sono state aggiunte delle flag che riguardano variabili d'ambiente ed è stato scelto per evitare di modificare file di configurazione rendendo meno limpida la riproducibilità.

#MapReduce esecuzione locale
time mapred streaming \
  -jt local \
  -fs file:/// \
  -files mapper.py,reducer.py \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -input file://$(pwd)/NOME_DATASET.csv \
  -output file://$(pwd)/output_mr_local_job3_1

#MapReduce con flag per la scelta del numero di Reducer
time hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar \
  -D yarn.app.mapreduce.am.env="HADOOP_MAPRED_HOME=$HADOOP_HOME" \
  -D mapreduce.map.env="HADOOP_MAPRED_HOME=$HADOOP_HOME" \
  -D mapreduce.reduce.env="HADOOP_MAPRED_HOME=$HADOOP_HOME" \
  -files mapper.py,reducer.py \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -input /user/hadoop/job3.1/input/NOME_DATASET.csv \
  -output /user/hadoop/job3.1/output_mr \
  -numReduceTasks 3

#Hive esecuzione locale 
time hive   -hiveconf mapreduce.framework.name=local   -f job3_1.hql

#Hive con flag per la scelta del numero di Reducer 
time hive \
  -hiveconf yarn.app.mapreduce.am.env="HADOOP_MAPRED_HOME=$HADOOP_HOME" \
  -hiveconf mapreduce.map.env="HADOOP_MAPRED_HOME=$HADOOP_HOME" \
  -hiveconf mapreduce.reduce.env="HADOOP_MAPRED_HOME=$HADOOP_HOME" \
  -hiveconf mapreduce.job.reduces=3 \
  -f job3_1.hql

#SparkCore esecuzione locale
time spark-submit --master local[*] job3_1_spark.py

#Queste flag spiegate qui di seguito sono state aggiunte dopo aver fallito diverse escuzioni per problemi di memoria rigurdanti il dataset completo.
#--num-executors 1, --executor-memory 1g, --driver-memory 1g
#A cosa servono: Limitano le risorse (RAM e Container).
#In ambiente locale o pseudo-distribuito, se lasciassimo fare a Spark, cercherebbe di assorbire tutta la RAM del PC causandone il blocco totale (OOM - Out Of Memory), riscontrato in diversi esperimenti. Questi flag impongono un limite di sicurezza a 1 Gigabyte.
#--conf spark.yarn.executor.memoryOverhead=1g
#A cosa serve: È un "cuscinetto" di RAM extra.
#YARN è spietato: se un container supera anche di 1 MB la memoria assegnata, lo uccide all'istante (Killed by YARN). L'overhead fornisce a Spark spazio vitale per le variabili non-heap (come le stringhe in Python), prevenendo fallimenti silenziosi nei job pesanti.
#--conf spark.sql.shuffle.partitions=4 e --conf spark.default.parallelism=4
#A cosa servono: Controllano il numero di partizioni create durante gli Shuffle (es. le GROUP BY).
#Di default, Spark crea 200 partizioni per ogni aggregazione. Su dataset ridotti o su un WSL con pochi core logici, creare e gestire 200 micro-task comporta un overhead tremendo (burocrazia) che dilata i tempi a dismisura. Abbassarlo a 4 adatta il software all'hardware locale.
#SparkCore su cluster con Yarn
time spark-submit \
  --master yarn \
  --deploy-mode client \
  --num-executors 1 \
  --executor-memory 1g \
  --driver-memory 1g \
  --conf spark.yarn.executor.memoryOverhead=1g \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.default.parallelism=4 \
  job3_1_spark.py

#SparkSQL esecuzione locale 
time spark-submit --master local[*] job3_1_sparksql.py

#SparkSQL su cluster con Yarn 
time spark-submit \
  --master yarn \
  --deploy-mode client \
  --num-executors 1 \
  --executor-memory 1g \
  --driver-memory 1g \
  --conf spark.yarn.executor.memoryOverhead=1g \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.default.parallelism=4 \
  job3_1_sparksql.py

#Esecuzione su ambiente cluster AWS EMR: tutti gli script e i dataset risiedono direttamente sul bucket S3, al cui interno sono state definite delle cartelle per conseguire maggiore chiarezza.
#Le esecuzioni vengono lanciate tramite connessione SSH al nodo Master del cluster EMR. Come in locale, la cartella di output su S3 deve essere eliminata prima di ogni nuova esecuzione.

#Comando per collegarsi al master node del cluster EMR
ssh -i ~/.ssh/pem/emr-key-user.pem hadoop@DNS_pubblico_del_nodo_primario

#MapReduce 
time hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files s3://nome_bucket_s3/scripts/job3.1/mapper.py,s3://nome_bucket_s3/scripts/job3.1/reducer.py \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -input s3://nome_bucket_s3/input/job3.1/ \
  -output s3://nome_bucket_s3/output/job3_1_mr/

#Hive
time hive -f s3://nome_bucket_s3/scripts/job3.1/job3_1AWS.hql

#SparkCore locale
aws s3 cp s3://nome_bucket_s3/scripts/job3.1/job3_1_sparkAWS.py 
time spark-submit \
  --master "local[*]" \
  job3_1_sparkAWS.py

#SparkCore
time spark-submit s3://nome_bucket_s3/scripts/job3.1/job3_1_sparkAWS.py

#SparkSQ locale
aws s3 cp s3://nome_bucket_s3/scripts/job3.1/job3_1_sparksqlAWS.py 
time spark-submit \
  --master "local[*]" \
  job3_1_sparksqlAWS.py

#SparkSQL
time spark-submit s3://nome_bucket_s3/scripts/job3.1/job3_1_sparksqlAWS.py

#Comandi utili alla riuscita degli esperimenti e della risoluzione di alcuni errori riscontrati

#Conversione di mapper e reducer in script unix
dos2unix mapper.py reducer.py

#Lettura delle prime 20 righe dell'output 
hdfs dfs -cat /user/hadoop/job3.1/output/part-* | head -n 20

#Rimozione dell'output per esecuzioni in serie
hdfs dfs -rm -r /user/hadoop/job3.1/output 2>/dev/null

#Comando per uscire dalla safe mode, poichè spesso alla prima esecuzione dopo l'avvio di wsl il nodo stava in safe mode e non permetteva l'esecuzione
hdfs dfsadmin -safemode leave

#Risoluzione problema riscontrato con esecuzioni Hive 
rm -rf metastore_db derby.log
schematool -dbType derby -initSchema
