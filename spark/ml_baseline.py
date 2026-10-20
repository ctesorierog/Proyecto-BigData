"""Línea base MLlib: predecir si ArrDelay supera 15 minutos.

Entrena con 2006 y evalúa con 2007 cuando el dataset termina en 2008.
El año 2008 publicado es parcial; usar años consecutivos completos permite
una evaluación temporal sencilla. El muestreo mantiene viable el ejercicio
en el clúster local. Todas las rutas proceden de config/pipeline.yaml.
"""

import sys
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
from airline_stage import hdfs_zone  # noqa: E402


SAMPLE_FRACTION = 0.02
SEED = 42


def _prepare(flights, year):
    """Conserva solo vuelos aptos y predictores conocidos antes de despegar."""
    return (
        flights
        .filter((F.col("Year") == year) & F.col("apto_modelo"))
        .withColumn("_hhmm", F.expr("try_cast(CRSDepTime AS INT)"))
        .filter(
            F.col("_hhmm").between(0, 2359)
            & (F.col("_hhmm") % 100 < 60)
        )
        .withColumn(
            "salida_programada_minutos",
            (F.floor(F.col("_hhmm") / 100) * 60 + F.col("_hhmm") % 100).cast("double"),
        )
        .withColumn("label", (F.col("ArrDelay") > 15).cast("double"))
        .select(
            "Month", "DayOfWeek", "salida_programada_minutos", "Distance",
            "UniqueCarrier", "Origin", "Dest", "label",
        )
        .sample(withReplacement=False, fraction=SAMPLE_FRACTION, seed=SEED)
    )


def run(spark, cfg):
    """Entrena, evalúa y guarda métricas pequeñas en la zona de resultados."""
    curated = hdfs_zone(cfg, "curated")
    results = hdfs_zone(cfg, "results").rstrip("/")
    last_year = int(cfg["data"]["last_year"])
    first_year = int(cfg["data"]["first_year"])
    train_year, test_year = last_year - 2, last_year - 1
    if train_year < first_year:
        raise ValueError("Se necesitan dos años completos para el baseline temporal")

    flights = spark.read.parquet(curated)
    train = _prepare(flights, train_year).cache()
    test = _prepare(flights, test_year).cache()

    try:
        n_train, n_test = train.count(), test.count()
        if n_train == 0 or n_test == 0:
            raise ValueError(
                f"Sin datos para ML: {train_year}={n_train}, {test_year}={n_test}"
            )

        categorical = ["UniqueCarrier", "Origin", "Dest"]
        indexed = [f"{name}_idx" for name in categorical]
        encoded = [f"{name}_vec" for name in categorical]
        indexers = [
            StringIndexer(inputCol=name, outputCol=out, handleInvalid="keep")
            for name, out in zip(categorical, indexed)
        ]
        encoder = OneHotEncoder(
            inputCols=indexed, outputCols=encoded, handleInvalid="keep"
        )
        assembler = VectorAssembler(
            inputCols=[
                "Month", "DayOfWeek", "salida_programada_minutos", "Distance",
                *encoded,
            ],
            outputCol="features",
        )
        classifier = LogisticRegression(
            featuresCol="features", labelCol="label", maxIter=10, regParam=0.01
        )
        model = Pipeline(stages=[*indexers, encoder, assembler, classifier]).fit(train)
        predictions = model.transform(test).cache()
        try:
            auc = BinaryClassificationEvaluator(
                labelCol="label", rawPredictionCol="rawPrediction",
                metricName="areaUnderROC",
            ).evaluate(predictions)
            summary = predictions.agg(
                F.avg("label").alias("proporcion_retrasados"),
                F.avg(
                    (F.col("prediction") == F.col("label")).cast("double")
                ).alias("accuracy"),
            ).first()
        finally:
            predictions.unpersist()

        metrics = {
            "train_year": train_year,
            "test_year": test_year,
            "sample_fraction": SAMPLE_FRACTION,
            "train_rows": n_train,
            "test_rows": n_test,
            "auc_roc": float(auc),
            "accuracy": float(summary["accuracy"]),
            "test_positive_rate": float(summary["proporcion_retrasados"]),
        }
        target = f"{results}/spark/ml_baseline"
        spark.createDataFrame([metrics]).coalesce(1).write.mode("overwrite").json(target)
        return {"metrics": metrics, "path": target}
    finally:
        train.unpersist()
        test.unpersist()
