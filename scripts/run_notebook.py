"""Run a notebook headlessly inside the Jupyter container, in order, saving the
outputs into the notebook after every cell (so the results stay as evidence).
Stops at the first error.

    docker compose exec jupyter python work/scripts/run_notebook.py work/<notebook>.ipynb [cells]

cells: indices to run, e.g. "0-41" or "0-12,43-49" (default: all). Markdown cells are skipped.
"""
import glob, os, sys
import nbformat
from nbclient import NotebookClient
from nbclient.exceptions import CellExecutionError

spark_home = os.environ.get("SPARK_HOME", "/usr/local/spark")
os.environ["PYTHONPATH"] = ":".join([f"{spark_home}/python"] + glob.glob(f"{spark_home}/python/lib/py4j-*.zip"))

path = sys.argv[1]
wanted = None
if len(sys.argv) > 2:
    wanted = set()
    for part in sys.argv[2].split(","):
        a, _, b = part.partition("-")
        wanted.update(range(int(a), int(b or a) + 1))
nb = nbformat.read(path, as_version=4)
client = NotebookClient(nb, timeout=None, kernel_name="python3",
                        resources={"metadata": {"path": os.path.dirname(os.path.abspath(path))}})
with client.setup_kernel():
    for i, cell in enumerate(nb.cells):
        if cell.cell_type != "code" or (wanted is not None and i not in wanted):
            continue
        head = (cell.source.splitlines() or [""])[0][:70]
        print(f"[cell {i}] {head}", flush=True)
        try:
            client.execute_cell(cell, i)
        except CellExecutionError as e:
            nbformat.write(nb, path)
            print(f"\nFAILED at cell {i}:\n{str(e)[-1500:]}", flush=True)
            sys.exit(1)
        nbformat.write(nb, path)
print("done", flush=True)
