#!/usr/bin/env python3
import sys

current_key = None
total_flights = 0
cancelled_flights = 0
valid_delays_count = 0
sum_delays = 0.0
min_delay = float('inf')
max_delay = float('-inf')
months = set()

def print_stats():
    """Funzione helper per stampare le statistiche aggregate quando la chiave cambia."""
    if current_key:
        carrier, origin = current_key.split('_')
        cancellation_rate = (cancelled_flights / total_flights) * 100 if total_flights > 0 else 0
        avg_delay = (sum_delays / valid_delays_count) if valid_delays_count > 0 else 0
        
        # Gestione del caso in cui non ci siano ritardi validi (solo voli perfetti o tutti cancellati)
        min_d = min_delay if min_delay != float('inf') else 0
        max_d = max_delay if max_delay != float('-inf') else 0
        
        months_str = "-".join(sorted(list(months)))
        
        # Output finale tabellare per il report
        print(f"{carrier}\t{origin}\t{total_flights}\t{min_d:.2f}\t{max_d:.2f}\t{avg_delay:.2f}\t{cancellation_rate:.2f}%\t{months_str}")

# Ciclo principale di lettura
for line in sys.stdin:
    line = line.strip()
    key, value = line.split('\t', 1)
    arr_delay_str, cancelled_str, month_str = value.split(',')
    
    # Se la chiave cambia (e non è la prima riga), stampiamo i risultati della chiave precedente
    if current_key != key:
        if current_key is not None:
            print_stats()
            
        # Reset delle variabili per la nuova chiave
        current_key = key
        total_flights = 0
        cancelled_flights = 0
        valid_delays_count = 0
        sum_delays = 0.0
        min_delay = float('inf')
        max_delay = float('-inf')
        months = set()
    
    # --- AGGIORNAMENTO DELLE STATISTICHE ---
    total_flights += 1
    cancelled_flights += int(float(cancelled_str))
    months.add(month_str)
    
    # Se arr_delay_str non è vuoto, lo elaboriamo per min, max e avg
    if arr_delay_str != "":
        delay = float(arr_delay_str)
        valid_delays_count += 1
        sum_delays += delay
        if delay < min_delay:
            min_delay = delay
        if delay > max_delay:
            max_delay = delay

# Assicuriamoci di stampare l'ultima chiave rimasta in memoria alla fine del file
if current_key == key:
    print_stats()