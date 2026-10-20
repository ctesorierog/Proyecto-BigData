"""Paso `analysis` del pipeline de Carlos: tres consultas desde curated."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from airline_stage import hdfs_zone, query_1, query_2, query_3  # noqa: E402


def run(spark, cfg):
    curated_path = hdfs_zone(cfg, "curated")
    results_path = hdfs_zone(cfg, "results").rstrip("/")
    flights = spark.read.parquet(curated_path)
    minimum = int(cfg["min_flights_per_group"])

    outputs = {
        "consulta_1": query_1(flights, minimum),
        "consulta_2": query_2(flights, minimum),
        "consulta_3": query_3(flights, minimum),
    }
    paths = {}
    for name, result in outputs.items():
        target = f"{results_path}/spark/{name}"
        result.write.mode("overwrite").parquet(target)
        paths[name] = target
    return paths
