#!/usr/bin/env python3
"""REDUCE: suma por clave y emite
Año <TAB> Aerolínea <TAB> n_vuelos <TAB> suma_retraso <TAB> retraso_medio <TAB> pct_retrasados"""
import sys

def emit(key, s, n, late):
    year, carrier = key.split("|")
    sys.stdout.write("{}\t{}\t{}\t{}\t{:.4f}\t{:.4f}\n".format(year, carrier, n, s, s / n, 100.0 * late / n))

current, s, n, late = None, 0, 0, 0
for line in sys.stdin:
    key, v_sum, v_n, v_late = line.rstrip("\n").split("\t")
    if key != current:
        if current is not None:
            emit(current, s, n, late)
        current, s, n, late = key, 0, 0, 0
    s += int(v_sum); n += int(v_n); late += int(v_late)
if current is not None:
    emit(current, s, n, late)
