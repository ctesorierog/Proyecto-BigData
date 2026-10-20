"""Single entry point of the project pipeline.

    download -> ingest (HDFS raw) -> etl (curated Parquet) -> quality -> analysis -> ml

Run from the repo folder, with the cluster up (docker compose up -d):
    docker compose exec jupyter python work/scripts/run_pipeline.py
    docker compose exec jupyter python work/scripts/run_pipeline.py --steps ingest etl

All paths and parameters come from config/pipeline.yaml. Every step is
idempotent, so running the pipeline twice gives the same result.

The Spark steps live in spark/ (workstream 2). Each one is a file with a
function run(spark, cfg) that writes its output with overwrite semantics.
A step whose file does not exist yet is reported as "skipped".
"""
import argparse
import datetime
import importlib.util
import json
import sys
import time
import traceback
from pathlib import Path

import os
import glob

import yaml

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts"))

# Plain "python" (docker compose exec) does not get the PYTHONPATH that Jupyter
# kernels get, so point it at the PySpark bundled in SPARK_HOME.
_SPARK_HOME = os.environ.get("SPARK_HOME", "/usr/local/spark")
sys.path[1:1] = [f"{_SPARK_HOME}/python"] + glob.glob(f"{_SPARK_HOME}/python/lib/py4j-*.zip")

import download_data  # noqa: E402
import ingest_raw     # noqa: E402

STEPS = ["download", "ingest", "etl", "quality", "analysis", "ml"]
SPARK_STEPS = {                     # step -> file in spark/ with run(spark, cfg)
    "etl": "spark/etl.py",
    "quality": "spark/quality.py",
    "analysis": "spark/analysis.py",
    "ml": "spark/ml_baseline.py",
}

_spark = None


def get_spark(cfg):
    global _spark
    if _spark is None:
        from pyspark.sql import SparkSession
        _spark = (SparkSession.builder
                  .appName("pipeline")
                  .master(cfg["spark"]["master"])
                  .config("spark.hadoop.dfs.replication", str(cfg["hdfs"]["replication"]))
                  .getOrCreate())
    return _spark


def load_module(path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run_step(step, cfg):
    if step == "download":
        return download_data.run(cfg, REPO / cfg["data"]["local_dir"])
    if step == "ingest":
        return ingest_raw.run(get_spark(cfg), cfg, REPO / cfg["data"]["local_dir"])
    path = REPO / SPARK_STEPS[step]
    if not path.exists():
        print(f"  {path.relative_to(REPO)} not found yet")
        return "skipped"
    return load_module(path).run(get_spark(cfg), cfg)


def main():
    ap = argparse.ArgumentParser(description="Airline pipeline entry point")
    ap.add_argument("--config", default=REPO / "config" / "pipeline.yaml")
    ap.add_argument("--steps", nargs="*", choices=STEPS, default=STEPS)
    args = ap.parse_args()
    cfg = yaml.safe_load(open(args.config))

    started = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log = {"started": started, "steps": []}
    ok = True
    for step in [s for s in STEPS if s in args.steps]:      # always in pipeline order
        print(f"\n== {step}", flush=True)
        t0 = time.time()
        try:
            result, status = run_step(step, cfg), "ok"
            if result == "skipped":
                status = "skipped"
        except Exception:
            traceback.print_exc()
            result, status, ok = None, "failed", False
        secs = round(time.time() - t0, 1)
        print(f"   -> {status} in {secs}s")
        log["steps"].append({"step": step, "status": status, "seconds": secs, "result": result})
        if not ok:
            break                                          # later steps need this one

    out = REPO / "evidence" / "pipeline_runs" / f"run_{started}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(log, indent=2, default=str))
    print(f"\nRun log: {out.relative_to(REPO)}")
    if _spark is not None:
        _spark.stop()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
