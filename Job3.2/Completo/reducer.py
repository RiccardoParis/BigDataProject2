#!/usr/bin/env python3
import sys
from collections import defaultdict

# --- MODIFICA INIZIALE ---
# Creiamo un super-dizionario globale per raggruppare TUTTO in RAM
global_stats = defaultdict(lambda: {
    "tier_stats": {
        "LOW": {"count": 0, "sum_dep": 0.0, "sum_arr": 0.0},
        "MEDIUM": {"count": 0, "sum_dep": 0.0, "sum_arr": 0.0},
        "HIGH": {"count": 0, "sum_dep": 0.0, "sum_arr": 0.0}
    },
    "causes_count": defaultdict(int)
})

# Ora il loop principale NON usa più "current_key", ma carica tutto nel dizionario
for line in sys.stdin:
    line = line.strip()
    parts = line.split('\t')
    
    if len(parts) != 6:
        continue
        
    origin, month, tier, dep_delay_str, arr_delay_str, causes_str = parts
    key = f"{origin}\t{month}"
    
    # Raccogliamo i dati per questa chiave
    if tier in global_stats[key]["tier_stats"]:
        global_stats[key]["tier_stats"][tier]["count"] += 1
        global_stats[key]["tier_stats"][tier]["sum_dep"] += float(dep_delay_str)
        global_stats[key]["tier_stats"][tier]["sum_arr"] += float(arr_delay_str)
        
    if causes_str != "NONE":
        for cause in causes_str.split('|'):
            global_stats[key]["causes_count"][cause] += 1

# --- FINE MODIFICA: Ora stampiamo tutto ciclando il dizionario globale ---
for key, data in global_stats.items():
    origin, month = key.split('\t')
    tier_stats = data["tier_stats"]
    causes_count = data["causes_count"]
    
    # ... (qui metti esattamente la logica di calcolo delle medie e 
    # della top 3 che avevi prima, usando 'tier_stats' e 'causes_count')
    # Calcolo delle medie per ogni fascia
    avgs = {}
    for t in ["LOW", "MEDIUM", "HIGH"]:
        c = tier_stats[t]["count"]
        if c > 0:
            avg_dep = tier_stats[t]["sum_dep"] / c
            avg_arr = tier_stats[t]["sum_arr"] / c
        else:
            avg_dep = 0.0
            avg_arr = 0.0
        avgs[t] = (c, round(avg_dep, 2), round(avg_arr, 2))
        
    # Calcolo della Top 3 delle cause
    if "NONE" in causes_count:
        del causes_count["NONE"]
        
    sorted_causes = sorted(causes_count.items(), key=lambda x: x[1], reverse=True)
    top_3 = sorted_causes[:3]
    top_3_str = ", ".join([f"{k}({v})" for k, v in top_3])
    if not top_3_str:
        top_3_str = "Nessuna_Causa_Rilevata"
        
    out_vals = [
        origin, month,
        f"Low:{avgs['LOW'][0]} voli (DepAvg:{avgs['LOW'][1]}, ArrAvg:{avgs['LOW'][2]})",
        f"Med:{avgs['MEDIUM'][0]} voli (DepAvg:{avgs['MEDIUM'][1]}, ArrAvg:{avgs['MEDIUM'][2]})",
        f"High:{avgs['HIGH'][0]} voli (DepAvg:{avgs['HIGH'][1]}, ArrAvg:{avgs['HIGH'][2]})",
        f"Top3_Cause:[{top_3_str}]"
    ]
    print("\t".join(out_vals))