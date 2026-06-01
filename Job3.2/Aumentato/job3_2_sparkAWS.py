from pyspark import SparkContext, SparkConf
import sys

def parse_line(line):
    # Salta l'header
    if "origin" in line.lower(): return None
    fields = line.split(',')
    if len(fields) < 11: return None
    
    try:
        origin = fields[0]
        month = int(fields[1])
        dep_delay = float(fields[2]) if fields[2] else 0.0
        arr_delay = float(fields[3]) if fields[3] else 0.0
        cancelled = float(fields[4])
        code = fields[5]
        carrier = float(fields[6])
        weather = float(fields[7])
        nas = float(fields[8])
        security = float(fields[9])
        late = float(fields[10])
        
        return (origin, month), (dep_delay, arr_delay, cancelled, code, carrier, weather, nas, security, late)
    except:
        return None

def reduce_func(v1, v2):
    # v1 e v2 sono tuple di (fasce_stats, cause_counts)
    # v1[0] = {LOW: [c, d_sum, a_sum], ...}
    # v1[1] = {Causa1: count, ...}
    
    # Merge Stats
    new_stats = v1[0]
    for tier in ['LOW', 'MEDIUM', 'HIGH']:
        new_stats[tier][0] += v2[0][tier][0]
        new_stats[tier][1] += v2[0][tier][1]
        new_stats[tier][2] += v2[0][tier][2]
        
    # Merge Causes
    new_causes = v1[1]
    for cause, count in v2[1].items():
        new_causes[cause] = new_causes.get(cause, 0) + count
        
    return (new_stats, new_causes)

if __name__ == "__main__":
    conf = SparkConf().setAppName("Job3.2_SparkCore")
    sc = SparkContext(conf=conf)
    
    lines = sc.textFile("s3://nome_bucket_s3/input/job3.2/dataset_job_3_2_aumentato.csv")
    
    # Trasformazione: Mappa in (Chiave, (Stats, Causes))
    rdd = lines.map(parse_line).filter(lambda x: x is not None).map(lambda x: (x[0], (
        {
            'LOW': [1 if x[1][2]!=1 and x[1][0]>0 and x[1][0]<15 else 0, x[1][0] if x[1][2]!=1 and x[1][0]>0 and x[1][0]<15 else 0, x[1][1] if x[1][2]!=1 and x[1][0]>0 and x[1][0]<15 else 0],
            'MEDIUM': [1 if x[1][2]!=1 and x[1][0]>=15 and x[1][0]<=60 else 0, x[1][0] if x[1][2]!=1 and x[1][0]>=15 and x[1][0]<=60 else 0, x[1][1] if x[1][2]!=1 and x[1][0]>=15 and x[1][0]<=60 else 0],
            'HIGH': [1 if x[1][2]!=1 and x[1][0]>60 else 0, x[1][0] if x[1][2]!=1 and x[1][0]>60 else 0, x[1][1] if x[1][2]!=1 and x[1][0]>60 else 0]
        },
        {c: 1 for c in [
            'Cancellazione_'+x[1][3] if x[1][2]==1 and x[1][3] and x[1][3]!='N' else None,
            'Carrier_Delay' if x[1][4]>0 else None,
            'Weather_Delay' if x[1][5]>0 else None,
            'NAS_Delay' if x[1][6]>0 else None,
            'Security_Delay' if x[1][7]>0 else None,
            'Late_Aircraft_Delay' if x[1][8]>0 else None
        ] if c is not None}
    )))

    # Aggregazione
    result = rdd.reduceByKey(reduce_func).map(lambda x: (
        x[0], 
        x[1][0], 
        sorted(x[1][1].items(), key=lambda y: y[1], reverse=True)[:3]
    ))
    
    # 1. Aggregazione: Mantiene la struttura (Key, Value) -> ((origin, month), (stats, causes))
    # 'reduce_func' deve essere definita come prima
    result_rdd = rdd.reduceByKey(reduce_func)

    # 2. Ordiniamo PRIMA di formattare: Spark accetta (Key, Value) per il sortByKey
    # In questo modo l'ordine globale è garantito dalla chiave (origin, month)
    sorted_rdd = result_rdd.sortByKey()

    # 3. Ora formattiamo (Map): trasformiamo la coppia in stringa finale
    def format_row(row):
        (org, m), (stats, causes_dict) = row
        # Calcoliamo la Top 3 qui, dentro il map
        top3 = sorted(causes_dict.items(), key=lambda y: y[1], reverse=True)[:3]
        
        low_s = f"Low:{stats['LOW'][0]} voli (DepAvg:{round(stats['LOW'][1]/stats['LOW'][0],2) if stats['LOW'][0]>0 else 0.0}, ArrAvg:{round(stats['LOW'][2]/stats['LOW'][0],2) if stats['LOW'][0]>0 else 0.0})"
        med_s = f"Med:{stats['MEDIUM'][0]} voli (DepAvg:{round(stats['MEDIUM'][1]/stats['MEDIUM'][0],2) if stats['MEDIUM'][0]>0 else 0.0}, ArrAvg:{round(stats['MEDIUM'][2]/stats['MEDIUM'][0],2) if stats['MEDIUM'][0]>0 else 0.0})"
        high_s = f"High:{stats['HIGH'][0]} voli (DepAvg:{round(stats['HIGH'][1]/stats['HIGH'][0],2) if stats['HIGH'][0]>0 else 0.0}, ArrAvg:{round(stats['HIGH'][2]/stats['HIGH'][0],2) if stats['HIGH'][0]>0 else 0.0})"
        causes_s = ", ".join([f"{k}({v})" for k, v in top3]) if top3 else "Nessuna_Causa_Rilevata"
        return f"{org}\t{m}\t{low_s}\t{med_s}\t{high_s}\tTop3_Cause:[{causes_s}]"

    # Salva
    sorted_rdd.map(format_row).saveAsTextFile("s3://nome_bucket_s3/output_spark3_2")
    