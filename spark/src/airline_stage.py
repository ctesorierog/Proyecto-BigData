"""Etapa Spark de Felipe: lectura, aptitud y tres consultas de vuelos.

Las rutas se reciben como argumentos; el notebook de integración las lee del YAML.
No escribe datos ni crea una SparkSession al importar este módulo.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession, Window, functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


FLIGHT_COLUMNS = (
    "Year", "Month", "DayofMonth", "DayOfWeek", "DepTime", "CRSDepTime",
    "ArrTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum",
    "ActualElapsedTime", "CRSElapsedTime", "AirTime", "ArrDelay", "DepDelay",
    "Origin", "Dest", "Distance", "TaxiIn", "TaxiOut", "Cancelled",
    "CancellationCode", "Diverted", "CarrierDelay", "WeatherDelay",
    "NASDelay", "SecurityDelay", "LateAircraftDelay",
)

INTEGER_COLUMNS = {
    "Year", "Month", "DayofMonth", "DayOfWeek", "ActualElapsedTime",
    "CRSElapsedTime", "AirTime", "ArrDelay", "DepDelay", "Distance",
    "TaxiIn", "TaxiOut", "Cancelled", "Diverted", "CarrierDelay",
    "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay",
}

FLIGHT_SCHEMA = StructType([
    StructField(name, IntegerType() if name in INTEGER_COLUMNS else StringType(), True)
    for name in FLIGHT_COLUMNS
])

AIRPORT_SCHEMA = StructType([
    StructField(name, StringType(), True)
    for name in ("iata", "airport", "city", "state", "country", "lat", "long")
])

CARRIER_SCHEMA = StructType([
    StructField(name, StringType(), True)
    for name in ("Code", "Description")
])

ANNUAL_FILE = re.compile(r"^((?:19|20)\d{2})\.csv(?:\.bz2)?$", re.IGNORECASE)
REQUIRED_YEARS = set(range(1987, 2009))


@dataclass(frozen=True)
class RawFiles:
    flights: dict[int, str]
    airports: str | None
    carriers: str | None


def hdfs_zone(cfg: dict, name: str) -> str:
    """Construye la URI HDFS a partir del contrato real de config/pipeline.yaml."""
    base = cfg["hdfs"]["url"].rstrip("/")
    zone = cfg["zones"][name]
    if not zone.startswith("/"):
        raise ValueError(f"zones.{name} debe ser una ruta absoluta de HDFS: {zone}")
    return base + zone


def discover_raw_files(spark: SparkSession, raw_path: str) -> RawFiles:
    """Descubre CSV anuales, incluso dentro de carpetas como 2001.csv/2001.csv."""
    jpath = spark._jvm.org.apache.hadoop.fs.Path(raw_path)
    filesystem = jpath.getFileSystem(spark._jsc.hadoopConfiguration())
    if not filesystem.exists(jpath):
        raise FileNotFoundError(f"La zona raw no existe o no es accesible: {raw_path}")

    files = filesystem.listFiles(jpath, True)
    flights: dict[int, str] = {}
    airports = None
    carriers = None
    while files.hasNext():
        path = files.next().getPath().toString()
        name = path.rsplit("/", 1)[-1]
        match = ANNUAL_FILE.fullmatch(name)
        if match:
            year = int(match.group(1))
            if year in REQUIRED_YEARS:
                if year in flights:
                    raise ValueError(f"Hay más de un CSV para {year}: {flights[year]} y {path}")
                flights[year] = path
        elif name.lower() == "airports.csv":
            if airports is not None:
                raise ValueError(f"Hay más de un airports.csv: {airports} y {path}")
            airports = path
        elif name.lower() == "carriers.csv":
            if carriers is not None:
                raise ValueError(f"Hay más de un carriers.csv: {carriers} y {path}")
            carriers = path

    if not flights:
        raise FileNotFoundError(f"No encontré CSV anuales 1987–2008 bajo {raw_path}")
    return RawFiles(flights, airports, carriers)


def read_flights(spark: SparkSession, files: RawFiles, years: Iterable[int] | None = None) -> DataFrame:
    """Lee los años elegidos con el esquema original y anota su año de archivo."""
    if years is None:
        absent = sorted(REQUIRED_YEARS - files.flights.keys())
        if absent:
            raise FileNotFoundError(f"Faltan años para la lectura completa 1987–2008: {absent}")
    selected = sorted(files.flights if years is None else set(years))
    if not selected:
        raise ValueError("Selecciona al menos un año")
    missing = sorted(set(selected) - files.flights.keys())
    if missing:
        raise FileNotFoundError(f"No encontré los CSV de los años: {missing}")

    combined = None
    for year in selected:
        frame = (
            spark.read.schema(FLIGHT_SCHEMA)
            .option("header", "true")
            .option("enforceSchema", "false")
            .option("nullValue", "NA")
            .csv(files.flights[year])
            .select(
                "Year", "Month", "DayOfWeek", "CRSDepTime", "UniqueCarrier",
                "Origin", "Dest", "Distance", "ArrDelay", "Cancelled", "Diverted",
            )
            .withColumn("_anio_fuente", F.lit(year))
        )
        combined = frame if combined is None else combined.unionByName(frame)
    return combined


def read_airports(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.schema(AIRPORT_SCHEMA)
        .option("header", "true")
        .option("enforceSchema", "false")
        .option("nullValue", "NA")
        .csv(path)
    )


def read_carriers(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.schema(CARRIER_SCHEMA)
        .option("header", "true")
        .option("enforceSchema", "false")
        .option("nullValue", "NA")
        .csv(path)
    )


def _nonblank(name: str):
    return F.col(name).isNotNull() & (F.trim(F.col(name)) != "")


def mark_aptitude(flights: DataFrame) -> DataFrame:
    """Aplica las reglas acordadas sin descartar vuelos útiles para otras salidas."""
    scheduled_hhmm = F.expr("try_cast(CRSDepTime AS INT)")
    valid_scheduled_time = (
        _nonblank("CRSDepTime")
        & F.col("CRSDepTime").rlike(r"^\d{1,4}$")
        & scheduled_hhmm.between(0, 2359)
        & (scheduled_hhmm % 100 < 60)
    )
    flights = flights.withColumn(
        "motivo_vuelo",
        F.when(F.col("Cancelled").isNull() | F.col("Diverted").isNull(), "estado no interpretable")
        .when(~F.col("Cancelled").isin(0, 1) | ~F.col("Diverted").isin(0, 1), "estado fuera de rango")
        .when(F.col("Cancelled") == 1, "cancelado")
        .when(F.col("Diverted") == 1, "desviado")
        .when(F.col("ArrDelay").isNull(), "ArrDelay no disponible"),
    ).withColumn("vuelo_valido", F.col("motivo_vuelo").isNull())

    base_reason = (
        F.when(~F.col("vuelo_valido"), F.col("motivo_vuelo"))
        .when(
            F.col("Year").isNull() | (F.col("Year") != F.col("_anio_fuente")),
            "Year no coincide con archivo",
        )
    )
    flights = (
        flights
        .withColumn(
            "motivo_consulta_1",
            base_reason.when(~_nonblank("UniqueCarrier"), "UniqueCarrier no disponible"),
        )
        .withColumn("apto_consulta_1", F.col("motivo_consulta_1").isNull())
        .withColumn(
            "motivo_consulta_2",
            base_reason
            .when(~_nonblank("Origin"), "Origin no disponible")
            .when(~_nonblank("Dest"), "Dest no disponible"),
        )
        .withColumn("apto_consulta_2", F.col("motivo_consulta_2").isNull())
        .withColumn(
            "motivo_consulta_3",
            base_reason
            .when(~_nonblank("Origin"), "Origin no disponible")
            .when(~_nonblank("UniqueCarrier"), "UniqueCarrier no disponible"),
        )
        .withColumn("apto_consulta_3", F.col("motivo_consulta_3").isNull())
        .withColumn(
            "motivo_modelo",
            base_reason
            .when(F.col("Month").isNull() | ~F.col("Month").between(1, 12), "Month fuera de rango")
            .when(F.col("DayOfWeek").isNull() | ~F.col("DayOfWeek").between(1, 7), "DayOfWeek fuera de rango")
            .when(~valid_scheduled_time, "CRSDepTime no válida")
            .when(~_nonblank("UniqueCarrier"), "UniqueCarrier no disponible")
            .when(~_nonblank("Origin"), "Origin no disponible")
            .when(~_nonblank("Dest"), "Dest no disponible")
            .when(F.col("Distance").isNull() | (F.col("Distance") <= 0), "Distance no válida"),
        )
        .withColumn("apto_modelo", F.col("motivo_modelo").isNull())
    )
    return flights


def query_1(flights: DataFrame, min_group_size: int = 100) -> DataFrame:
    """Retraso medio por aerolínea y año; grupos de al menos 100 vuelos."""
    return (
        flights.filter(F.col("apto_consulta_1"))
        .groupBy("Year", "UniqueCarrier")
        .agg(F.count("*").alias("n_vuelos"), F.avg("ArrDelay").alias("retraso_medio"))
        .filter(F.col("n_vuelos") >= min_group_size)
        .orderBy("Year", "UniqueCarrier")
    )


def query_2(flights: DataFrame, min_group_size: int = 100) -> DataFrame:
    """Tres rutas de mayor retraso medio dentro de cada aeropuerto de origen."""
    routes = (
        flights.filter(F.col("apto_consulta_2"))
        .groupBy("Origin", "Dest")
        .agg(F.count("*").alias("n_vuelos"), F.avg("ArrDelay").alias("retraso_medio"))
        .filter(F.col("n_vuelos") >= min_group_size)
    )
    rank = Window.partitionBy("Origin").orderBy(F.desc("retraso_medio"), F.asc("Dest"))
    return (
        routes.withColumn("puesto", F.row_number().over(rank))
        .filter(F.col("puesto") <= 3)
        .orderBy("Origin", "puesto")
    )


def query_3(flights: DataFrame, min_group_size: int = 100) -> DataFrame:
    """Media de aerolínea-origen frente a la media ponderada por vuelos del origen."""
    groups = (
        flights.filter(
            F.col("vuelo_valido")
            & F.col("Year").isNotNull()
            & (F.col("Year") == F.col("_anio_fuente"))
            & _nonblank("Origin")
        )
        .groupBy("Origin", "UniqueCarrier")
        .agg(F.count("*").alias("n_vuelos"), F.sum("ArrDelay").alias("suma_retrasos"))
    )
    origin = Window.partitionBy("Origin")
    return (
        groups
        .withColumn("_suma_origen", F.sum("suma_retrasos").over(origin))
        .withColumn("_n_origen", F.sum("n_vuelos").over(origin))
        .filter(_nonblank("UniqueCarrier") & (F.col("n_vuelos") >= min_group_size))
        .select(
            "Origin", "UniqueCarrier", "n_vuelos",
            (F.col("suma_retrasos") / F.col("n_vuelos")).alias("retraso_medio_aerolinea"),
            (F.col("_suma_origen") / F.col("_n_origen")).alias("retraso_medio_origen"),
            ((F.col("suma_retrasos") / F.col("n_vuelos"))
             - (F.col("_suma_origen") / F.col("_n_origen"))).alias("diferencia_minutos"),
        )
        .orderBy(F.desc("diferencia_minutos"), "Origin", "UniqueCarrier")
    )
