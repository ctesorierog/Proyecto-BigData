"""Paso `etl` del pipeline de Carlos: CSV raw -> Parquet curated por Year."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from airline_stage import discover_raw_files, hdfs_zone, mark_aptitude, read_flights  # noqa: E402


def run(spark, cfg):
    raw_path = hdfs_zone(cfg, "raw_flights")
    curated_path = hdfs_zone(cfg, "curated")
    years = range(cfg["data"]["first_year"], cfg["data"]["last_year"] + 1)
    files = discover_raw_files(spark, raw_path)
    flights = mark_aptitude(read_flights(spark, files, years))

    partition_column = cfg["partition_column"]
    if partition_column != "Year":
        raise ValueError(f"La partición acordada es Year; recibí {partition_column!r}")
    flights.write.mode("overwrite").partitionBy(partition_column).parquet(curated_path)
    return {"curated": curated_path, "years": len(years), "partition_column": partition_column}
