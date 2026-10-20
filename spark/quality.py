"""Controles de calidad por año sobre los vuelos curated.

Las filas canceladas o desviadas no son errores: se cuentan por separado.
No se descartan vuelos aquí; las marcas de aptitud del ETL deciden qué salida
puede utilizar cada fila. El resultado se reescribe en cada ejecución.
"""

import sys
from pathlib import Path

from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
from airline_stage import hdfs_zone  # noqa: E402


def _count_if(condition, name):
    return F.sum(F.when(condition, 1).otherwise(0)).cast("long").alias(name)


def run(spark, cfg):
    """Resume nulos, rangos y aptitud sin confundir exclusión con corrupción."""
    curated = hdfs_zone(cfg, "curated")
    results = hdfs_zone(cfg, "results").rstrip("/")
    flights = spark.read.parquet(curated)

    report = flights.groupBy("Year").agg(
        F.count("*").alias("filas"),
        _count_if(F.col("Year").isNull() | (F.col("Year") != F.col("_anio_fuente")), "year_invalido"),
        _count_if(F.col("Cancelled").isNull() | F.col("Diverted").isNull(), "estado_nulo"),
        _count_if(
            (F.col("Cancelled").isNotNull() & ~F.col("Cancelled").isin(0, 1))
            | (F.col("Diverted").isNotNull() & ~F.col("Diverted").isin(0, 1)),
            "estado_fuera_de_rango",
        ),
        _count_if(F.col("Cancelled") == 1, "cancelados"),
        _count_if(F.col("Diverted") == 1, "desviados"),
        _count_if(F.col("ArrDelay").isNull(), "arrdelay_nulo"),
        _count_if(F.col("Month").isNull() | ~F.col("Month").between(1, 12), "month_invalido"),
        _count_if(F.col("DayOfWeek").isNull() | ~F.col("DayOfWeek").between(1, 7), "dayofweek_invalido"),
        _count_if(F.col("Distance").isNull() | (F.col("Distance") <= 0), "distance_invalida"),
        _count_if(F.col("vuelo_valido"), "vuelos_validos"),
        *(
            _count_if(F.col(name), name)
            for name in ("apto_consulta_1", "apto_consulta_2", "apto_consulta_3", "apto_modelo")
        ),
    ).coalesce(1)

    target = f"{results}/spark/quality"
    report.cache()
    try:
        yearly = report.collect()
        report.write.mode("overwrite").json(target)
    finally:
        report.unpersist()
    totals = {k: int(sum(row[k] for row in yearly)) for k in yearly[0].asDict() if k != "Year"}
    n = totals["filas"]
    expected_years = int(cfg["data"]["last_year"]) - int(cfg["data"]["first_year"]) + 1
    # Acción definida para cada control: stop (detiene el pipeline), warn (aviso) o log (registro)
    unexpected = ["year_invalido", "estado_nulo", "estado_fuera_de_rango",
                  "month_invalido", "dayofweek_invalido", "distance_invalida"]
    expected_exclusions = ["cancelados", "desviados", "arrdelay_nulo"]
    stop_pct = float(cfg.get("quality_stop_pct", 5.0))
    actions = []
    if n == 0 or len(yearly) != expected_years:
        actions.append({"control": "años_y_filas", "valor": [len(yearly), n], "accion": "stop"})
    for c in unexpected:
        pct = 100.0 * totals[c] / n if n else 0.0
        accion = "stop" if pct > stop_pct else ("warn" if totals[c] > 0 else "ok")
        actions.append({"control": c, "valor": totals[c], "pct": round(pct, 4), "accion": accion})
    for c in expected_exclusions:
        actions.append({"control": c, "valor": totals[c], "accion": "log"})
    for a in actions:
        if a["accion"] == "warn":
            print(f"  QUALITY WARN {a['control']}: {a['valor']} filas ({a['pct']} %)", flush=True)
    stops = [a for a in actions if a["accion"] == "stop"]
    if stops:
        raise RuntimeError(f"QUALITY STOP: {stops}")   # run_pipeline marca el paso como fallido y se detiene
    return {"path": target, "years": len(yearly), "totals": totals, "actions": actions}
