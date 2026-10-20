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
    return {
        "path": target,
        "years": len(yearly),
        "rows": sum(row["filas"] for row in yearly),
        "controls": [
            "year", "estado", "ArrDelay", "Month", "DayOfWeek", "Distance",
            "aptitud_por_salida",
        ],
    }
