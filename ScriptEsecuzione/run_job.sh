#!/bin/bash

# Controllo sul numero di argomenti passati
if [ "$#" -lt 4 ]; then
    echo "=========================================================================="
    echo " ERRORE: Parametri mancanti."
    echo " Uso corretto: ./run_job.sh <tecnologia> <input> <output> <script>"
    echo ""
    echo " Tecnologie supportate: mr (MapReduce), hive, spark, sparksql"
    echo ""
    echo " Esempi di utilizzo:"
    echo "   ./run_job.sh mr /dataset.csv /out_mr mapper.py,reducer.py"
    echo "   ./run_job.sh hive /dataset.csv /out_hive job3_1.hql"
    echo "   ./run_job.sh sparksql /dataset.csv /out_spark job3_1_sparksql.py"
    echo "=========================================================================="
    exit 1
fi

TECH=$1
INPUT=$2
OUTPUT=$3
SCRIPTS=$4

echo "=========================================="
echo " Avvio Job Big Data"
echo " Tecnologia : $TECH"
echo " Input      : $INPUT"
echo " Output     : $OUTPUT"
echo " Script(s)  : $SCRIPTS"
echo "=========================================="

# 1. Rimuove l'output precedente per evitare errori "File already exists" su HDFS
echo "[-] Pulizia della cartella di output HDFS precedente..."
hdfs dfs -rm -r -f $OUTPUT 2>/dev/null

# 2. Esecuzione instradata in base alla tecnologia
if [ "$TECH" == "mr" ]; then
    # Per MapReduce separiamo i file del Mapper e del Reducer passati con la virgola
    MAPPER=$(echo $SCRIPTS | cut -d',' -f1)
    REDUCER=$(echo $SCRIPTS | cut -d',' -f2)
    
    time hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
        -D yarn.app.mapreduce.am.env="HADOOP_MAPRED_HOME=$HADOOP_HOME" \
        -D mapreduce.map.env="HADOOP_MAPRED_HOME=$HADOOP_HOME" \
        -D mapreduce.reduce.env="HADOOP_MAPRED_HOME=$HADOOP_HOME" \
        -files $MAPPER,$REDUCER \
        -mapper "python3 $MAPPER" \
        -reducer "python3 $REDUCER" \
        -input $INPUT \
        -output $OUTPUT

elif [ "$TECH" == "hive" ]; then
    # In Hive passiamo input e output come variabili interne (--hivevar)
    # E impostiamo le variabili d'ambiente di MapReduce direttamente nella configurazione di Hive
    time hive \
        --hiveconf yarn.app.mapreduce.am.env="HADOOP_MAPRED_HOME=$HADOOP_HOME" \
        --hiveconf mapreduce.map.env="HADOOP_MAPRED_HOME=$HADOOP_HOME" \
        --hiveconf mapreduce.reduce.env="HADOOP_MAPRED_HOME=$HADOOP_HOME" \
        --hivevar INPUT_PATH=$INPUT \
        --hivevar OUTPUT_PATH=$OUTPUT \
        -f $SCRIPTS

elif [ "$TECH" == "spark" ] || [ "$TECH" == "sparksql" ]; then
    # In Spark passiamo input e output come argomenti al termine del comando
    time spark-submit $SCRIPTS $INPUT $OUTPUT

else
    echo "[!] Tecnologia '$TECH' non riconosciuta. Esecuzione annullata."
    exit 1
fi

echo "=========================================="
echo " Job Terminato con successo!"
echo "=========================================="