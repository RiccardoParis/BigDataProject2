#!/usr/bin/env python3
import sys

# Leggiamo da standard input riga per riga
for line in sys.stdin:
    line = line.strip()
    
    # Ignoriamo l'intestazione
    if line.startswith('op_unique_carrier'):
        continue
        
    # Splittiamo il CSV (ci aspettiamo 5 colonne come dal tuo dataset pulito)
    # [op_unique_carrier, origin, arr_delay, cancelled, month]
    parts = line.split(',')
    
    if len(parts) == 5:
        carrier = parts[0]
        origin = parts[1]
        arr_delay = parts[2] # Può essere vuoto per voli cancellati/deviati
        cancelled = parts[3]
        month = parts[4]
        
        # Creiamo la chiave composta
        key = f"{carrier}_{origin}"
        
        # Stampiamo chiave e valori separati da tabulazione
        print(f"{key}\t{arr_delay},{cancelled},{month}")