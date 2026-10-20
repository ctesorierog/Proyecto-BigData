"""Download the Airline On-Time dataset (1987-2008) from Harvard Dataverse.

Reproducible: public API, no account or API key needed.
Idempotent: files already present in data/ are skipped, so running it twice
downloads nothing the second time. Partial downloads are written as *.part
and only renamed when complete, so an interrupted run never leaves a broken CSV.

Standalone use (from the repo folder):
    docker compose exec jupyter python work/scripts/download_data.py
    docker compose exec jupyter python work/scripts/download_data.py --dest /tmp/dltest --only carriers.csv
"""
import argparse
import bz2
import json
import shutil
import urllib.request
from pathlib import Path

API = "https://dataverse.harvard.edu/api"
# Dataverse rejects (403) requests with Python's default user agent
HEADERS = {"User-Agent": "Mozilla/5.0 (Proyecto-BigData course project; airline dataset download)"}
LOOKUPS = {"airports.csv", "carriers.csv"}


def wanted_files(cfg):
    """Names as published on Dataverse: one .csv.bz2 per year + the lookup tables."""
    years = range(cfg["data"]["first_year"], cfg["data"]["last_year"] + 1)
    return {f"{y}.csv.bz2" for y in years} | LOOKUPS


def local_name(remote_name):
    """1987.csv.bz2 is stored decompressed as 1987.csv."""
    return remote_name[:-4] if remote_name.endswith(".bz2") else remote_name


def list_remote_files(doi):
    """Ask Dataverse which files the dataset has: (name, file id, is_tabular)."""
    url = f"{API}/datasets/:persistentId/?persistentId={doi}"
    with urllib.request.urlopen(urllib.request.Request(url, headers=HEADERS), timeout=60) as resp:
        meta = json.load(resp)
    for f in meta["data"]["latestVersion"]["files"]:
        df = f["dataFile"]
        # Dataverse converts some CSVs to .tab; originalFileName keeps the real name
        yield df.get("originalFileName", df["filename"]), df["id"], "originalFileFormat" in df


def fetch(file_id, tabular, dest):
    url = f"{API}/access/datafile/{file_id}" + ("?format=original" if tabular else "")
    part = dest.with_name(dest.name + ".part")
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=600) as resp, open(part, "wb") as out:
        shutil.copyfileobj(resp, out, 1 << 20)
    part.rename(dest)


def decompress(src, dest):
    part = dest.with_name(dest.name + ".part")
    with bz2.open(src, "rb") as fin, open(part, "wb") as fout:
        shutil.copyfileobj(fin, fout, 1 << 20)
    part.rename(dest)


def run(cfg, dest, only=None):
    dest = Path(dest)
    dest.mkdir(parents=True, exist_ok=True)
    wanted = wanted_files(cfg)
    if only:
        wanted &= set(only) | {n + ".bz2" for n in only}

    missing = {n for n in wanted if not (dest / local_name(n)).exists()}
    print(f"  {len(wanted) - len(missing)}/{len(wanted)} files already in {dest}")
    if not missing:
        return {"downloaded": [], "skipped": len(wanted)}

    downloaded = []
    for name, file_id, tabular in list_remote_files(cfg["data"]["source_doi"]):
        if name not in missing:
            continue
        final = dest / local_name(name)
        print(f"  downloading {name} ...", flush=True)
        if name.endswith(".bz2"):
            tmp = dest / name
            fetch(file_id, tabular, tmp)
            decompress(tmp, final)
            tmp.unlink()
        else:
            fetch(file_id, tabular, final)
        downloaded.append(final.name)
        missing.discard(name)

    if missing:
        raise RuntimeError(f"Not found on Dataverse: {sorted(missing)}")
    return {"downloaded": downloaded, "skipped": len(wanted) - len(downloaded)}


if __name__ == "__main__":
    import yaml
    repo = Path(__file__).resolve().parents[1]
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--config", default=repo / "config" / "pipeline.yaml")
    ap.add_argument("--dest", help="target folder (default: data/ from the config)")
    ap.add_argument("--only", nargs="*", help="only these files, e.g. carriers.csv 1987.csv")
    args = ap.parse_args()
    cfg = yaml.safe_load(open(args.config))
    print(run(cfg, args.dest or repo / cfg["data"]["local_dir"], args.only))
