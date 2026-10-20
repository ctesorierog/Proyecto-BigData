#!/usr/bin/env python3
"""COMBINER: suma parcial dentro de cada tarea de map, antes del shuffle.
Entrada y salida con el mismo formato que el mapper."""
import sys

def emit(key, s, n, late):
    sys.stdout.write("{}\t{}\t{}\t{}\n".format(key, s, n, late))

current, s, n, late = None, 0, 0, 0
for line in sys.stdin:
    key, v_sum, v_n, v_late = line.rstrip("\n").split("\t")
    if key != current:                                 # las claves llegan ordenadas
        if current is not None:
            emit(current, s, n, late)
        current, s, n, late = key, 0, 0, 0
    s += int(v_sum); n += int(v_n); late += int(v_late)
if current is not None:
    emit(current, s, n, late)
