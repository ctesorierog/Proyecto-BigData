#!/usr/bin/env python3
"""MAP: cada línea del CSV -> 'Año|Aerolínea <TAB> retraso <TAB> 1 <TAB> retrasado(0/1)'."""
import io
import sys

YEAR, CARRIER, ARR_DELAY, CANCELLED, DIVERTED = 0, 8, 14, 21, 23

for line in io.TextIOWrapper(sys.stdin.buffer, encoding="latin-1"):
    f = line.rstrip("\r\n").split(",")
    if len(f) < 24 or not f[YEAR].isdigit():           # cabecera ("Year"), línea rota o año vacío
        continue
    if f[CANCELLED] != "0" or f[DIVERTED] != "0":      # solo vuelos operados
        continue
    try:
        delay = int(float(f[ARR_DELAY]))               # "NA" -> ValueError
    except ValueError:
        continue
    late = 1 if delay > 15 else 0
    sys.stdout.write("{}|{}\t{}\t1\t{}\n".format(f[YEAR], f[CARRIER], delay, late))
