"""Load the local CSVs into the HDFS raw zone, untouched.

Idempotent: a file already in HDFS with the same size is skipped; a missing or
different one is uploaded to a hidden temporary name and then renamed, so the
raw zone never contains half-written files and a rerun never duplicates data.
Uses the Hadoop FileSystem API through Spark (Spark is an HDFS client).
"""
import time
from pathlib import Path


def _hdfs(spark, url):
    jvm = spark._jvm
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(url),
                                                 spark._jsc.hadoopConfiguration())
    return fs, jvm.org.apache.hadoop.fs.Path


def wait_for_safe_mode(fs, timeout_s=180):
    """Right after a (re)start the NameNode is in safe mode and refuses writes
    until the DataNodes have reported their blocks. Wait instead of failing."""
    t0 = time.time()
    while fs.isInSafeMode():
        if time.time() - t0 > timeout_s:
            raise TimeoutError("NameNode still in safe mode after %ss" % timeout_s)
        print("  NameNode in safe mode, waiting...", flush=True)
        time.sleep(5)


def run(spark, cfg, data_dir):
    data_dir = Path(data_dir)
    fs, HPath = _hdfs(spark, cfg["hdfs"]["url"])
    wait_for_safe_mode(fs)
    zones = cfg["zones"]
    years = range(cfg["data"]["first_year"], cfg["data"]["last_year"] + 1)

    plan = [(data_dir / f"{y}.csv", zones["raw_flights"]) for y in years]
    plan += [(data_dir / n, zones["raw_lookup"]) for n in cfg["data"]["lookup_files"]]

    absent = [str(p) for p, _ in plan if not p.exists()]
    if absent:
        raise FileNotFoundError(f"Run the download step first, missing locally: {absent}")

    uploaded, skipped = [], []
    for local, target_dir in plan:
        dst = HPath(f"{target_dir}/{local.name}")
        size = local.stat().st_size
        if fs.exists(dst) and fs.getFileStatus(dst).getLen() == size:
            skipped.append(local.name)
            continue
        print(f"  uploading {local.name} -> {target_dir}/", flush=True)
        fs.mkdirs(HPath(target_dir))
        tmp = HPath(f"{target_dir}/.{local.name}.tmp")      # hidden: Spark ignores it
        fs.copyFromLocalFile(False, True, HPath("file://" + str(local)), tmp)
        if fs.exists(dst):
            fs.delete(dst, False)
        fs.rename(tmp, dst)
        uploaded.append(local.name)

    # Fail loudly: every expected file must now be in HDFS with the local size
    bad = [p.name for p, d in plan
           if fs.getFileStatus(HPath(f"{d}/{p.name}")).getLen() != p.stat().st_size]
    if bad:
        raise RuntimeError(f"Size mismatch in HDFS raw zone: {bad}")
    print(f"  raw zone OK: {len(plan)} files ({len(uploaded)} uploaded, {len(skipped)} already there)")
    total_bytes = sum(p.stat().st_size for p, _ in plan)
    return {"files": len(plan), "bytes": total_bytes,
            "uploaded": uploaded, "skipped": len(skipped)}
