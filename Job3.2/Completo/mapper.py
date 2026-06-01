#!/usr/bin/env python3
import sys

# Ordine colonne:
# 0:origin, 1:month, 2:dep_delay, 3:arr_delay, 4:cancelled, 5:cancellation_code,
# 6:carrier_delay, 7:weather_delay, 8:nas_delay, 9:security_delay, 10:late_aircraft_delay

for line in sys.stdin:
    line = line.strip()
    # Salta l'intestazione o le righe vuote
    if not line or line.startswith('origin'):
        continue
        
    parts = line.split(',')
    
    # Controllo di sicurezza sulla lunghezza
    if len(parts) < 11:
        continue
        
    origin = parts[0]
    month = parts[1]
    
    try:
        cancelled = float(parts[4]) if parts[4] else 0.0
    except ValueError:
        continue

    cancellation_code = parts[5]
    
    tier = "NONE"
    dep_delay = 0.0
    arr_delay = 0.0
    causes = []
    
    if cancelled == 1.0:
        tier = "CANCELLED"
        if cancellation_code and cancellation_code != 'N':
            causes.append(f"Cancellazione_{cancellation_code}")
    else:
        try:
            # Estraiamo i ritardi (se vuoti, mettiamo 0.0)
            dep_delay = float(parts[2]) if parts[2] else 0.0
            arr_delay = float(parts[3]) if parts[3] else 0.0
            
            # 1. Classificazione in fasce
            tier = None
            if  0 < dep_delay < 15:
                tier = "LOW"
            elif 15 <= dep_delay <= 60:
                tier = "MEDIUM"
            elif dep_delay > 60:
                tier = "HIGH"
                
            # 2. Identificazione cause di ritardo (> 0)
            delay_types = [
                (6, "Carrier_Delay"), 
                (7, "Weather_Delay"), 
                (8, "NAS_Delay"), 
                (9, "Security_Delay"), 
                (10, "Late_Aircraft_Delay")
            ]
            for idx, cause_name in delay_types:
                if parts[idx]:
                    val = float(parts[idx])
                    if val > 0:
                        causes.append(cause_name)
                        
        except ValueError:
            # Se ci sono problemi di conversione numerica, ignoriamo la riga
            pass
            
    # Uniamo le cause con un delimitatore speciale (il pipe '|') per separarle nel Reducer
    causes_str = "|".join(causes) if causes else "NONE"
    
    # La chiave sarà "Origin \t Month"
    key = f"{origin}\t{month}"
    
    # Il valore contiene Tier, Dep_Delay, Arr_Delay e la lista delle cause
    value = f"{tier}\t{dep_delay}\t{arr_delay}\t{causes_str}"
    
    if tier is not None:
        print(f"{key}\t{value}")