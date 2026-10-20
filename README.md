# Proyecto Big Data – Airline On-Time Performance (1987–2008)

Arquitectura Big Data local con **HDFS + Spark en Docker** sobre los vuelos entre aeropuertos de EE. UU. (Harvard Dataverse, `doi:10.7910/DVN/HG7NV7`): ~120 M de filas, ~11,5 GB en CSV. Un único comando levanta el clúster y otro ejecuta el pipeline completo, de la descarga a los resultados.

| Workstream | Responsable | Carpetas |
|---|---|---|
| 1. Infraestructura y pipeline | Carlos | `docker-compose.yml`, `config/`, `scripts/` |
| 2. Spark y ML | Felipe | `spark/` |
| 3. MapReduce, reto e informe | Odei | `mapreduce/`, `miembro3_mapreduce_reto.ipynb` |

`evidence/` es compartida: capturas, `fsck`, tiempos y logs de cada ejecución.

## Requisitos

- Docker Desktop con **≥ 8 GB de RAM** asignados (Settings → Resources).
- **≥ 46 GB de disco libre**: 11,5 GB de datos locales + ~23 GB en HDFS (replicación 2) + zona curated.
- Nada más: Python, PySpark y Hadoop vienen dentro de las imágenes.
- Probado en Windows 11 + Docker Desktop (Intel i7-13620H, 16 CPU y 7,6 GiB para Docker).
- **Linux/Mac:** Jupyter escribe en el repo como el usuario `jovyan` (uid 1000). Si tu usuario no es el 1000 (`id -u`), da permisos antes de arrancar: `chmod -R a+rwX data evidence mapreduce && chmod a+w *.ipynb`.

## Arquitectura del clúster

`docker compose up -d` arranca 7 contenedores en una red común:

| Servicio | Imagen | Función | Recursos |
|---|---|---|---|
| `namenode` | `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` + Python 3 (`docker/hadoop-python/Dockerfile`) | Maestro de HDFS: metadatos (qué bloques forman cada fichero y en qué DataNode están). Ve `data/` como `/data` y `mapreduce/` como `/opt/mapreduce` (solo lectura). También ejecuta Hadoop Streaming. | – |
| `datanode1`, `datanode2` | `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` | Guardan los bloques de 128 MB. Con `dfs.replication=2` cada bloque está en los dos. | – |
| `spark-master` | `quay.io/jupyter/pyspark-notebook@sha256:8043…` | Planificador de Spark standalone: reparte tareas entre los workers. | – |
| `spark-worker1`, `spark-worker2` | misma imagen | Ejecutan las tareas. | 2 cores · 2 GB ofrecidos cada uno (executor de 1 GB) |
| `jupyter` | misma imagen | Driver de PySpark (notebooks y scripts). Ve el repo como `/home/jovyan/work`. | driver 1 GB |

**Decisiones de diseño**

- **2 DataNodes y replicación 2**: es el mínimo con el que la replicación es real y el `fsck` muestra cada bloque en dos nodos. En un solo portátil más DataNodes no aceleran nada (comparten disco) y consumen RAM.
- **2 workers × 2 cores × 2 GB**: el reparto entre máquinas es visible y todo cabe en los 8 GB de Docker (~7 GB con HDFS, master y driver).
- **Misma imagen, fijada por digest, para master, workers y Jupyter**: driver y executors necesitan exactamente la misma versión de Spark (4.2.0) y de Python; el digest garantiza el mismo contenido en todos los portátiles.
- **Sin YARN**: Spark usa su planificador standalone y Hadoop Streaming se ejecuta en modo local dentro del NameNode. Ahorra ~2 GB de RAM.
- **NameNode con Python 3** (imagen propia, se construye sola con `docker compose up`): Hadoop Streaming necesita Python para el mapper y el reducer, y así no hay que instalar nada a mano. La imagen base es Debian 9, por eso es **Python 3.5** (sin f-strings en `mapreduce/*.py`).
- **Configuración de Hadoop en el propio Compose** (bloque `x-hadoop-env`), sin ficheros externos ni rutas personales: el repo funciona tal cual al clonarlo.
- `--conf spark.hadoop.dfs.replication=2`: por defecto un cliente Spark escribe 3 réplicas, más que DataNodes tenemos.
- Workers con `--work-dir /tmp/spark-work`: el usuario `jovyan` de la imagen no puede escribir en `/usr/local/spark/work`.
- `HADOOP_USER_NAME=root` y permisos HDFS desactivados: atajo de laboratorio, no para producción.

## Puesta en marcha

```bash
git clone https://github.com/ctesorierog/Proyecto-BigData.git
cd Proyecto-BigData
docker compose up -d          # la primera vez descarga las imágenes (~3 GB) y construye el NameNode
docker compose ps             # 7 contenedores "Up"
```

Si hay otro clúster usando los mismos puertos (p. ej. el `docker-hadoop` de clase), hay que pararlo antes.

| Interfaz | URL |
|---|---|
| HDFS NameNode | http://localhost:9870 (*Datanodes* → 2 en servicio) |
| Spark Master | http://localhost:8080 (2 workers ALIVE) |
| Spark Workers | http://localhost:8081, http://localhost:8082 |
| Jupyter | http://localhost:8888 – enlace con token: `docker compose logs jupyter` |
| Spark Application UI | http://localhost:4040 (solo con una sesión abierta) |

Parar: `docker compose down` (conserva HDFS) · `docker compose down -v` (borra también los volúmenes de HDFS).

Tras arrancar, el NameNode pasa unos segundos en *safe mode* (no admite escrituras hasta que los DataNodes informan de sus bloques). El pipeline espera solo; para comandos manuales: `docker compose exec namenode hdfs dfsadmin -safemode wait`.

## Ejecutar el pipeline

```bash
docker compose exec jupyter python work/scripts/run_pipeline.py
```

Pasos, siempre en este orden (se pueden elegir con `--steps`, p. ej. `--steps ingest etl`):

| Paso | Código | Qué hace |
|---|---|---|
| `download` | `scripts/download_data.py` | Descarga de Harvard Dataverse (API pública, sin cuenta ni clave) los 22 `AAAA.csv.bz2` + `airports.csv` y `carriers.csv`, y los descomprime en `data/`. |
| `ingest` | `scripts/ingest_raw.py` | Copia los CSV, sin tocarlos, a la zona raw de HDFS. |
| `etl` | `spark/etl.py` | Limpieza con esquema explícito → Parquet particionado por `Year` (zona curated). |
| `quality` | `spark/quality.py` | Controles de calidad: nulos, rangos, duplicados. |
| `analysis` | `spark/analysis.py` | Las 3 preguntas analíticas → zona results. |
| `ml` | `spark/ml_baseline.py` | Baseline de MLlib (`ArrDelay > 15`). |

Cada paso de `spark/` es un fichero con una función `run(spark, cfg)`; si el fichero aún no existe, el pipeline lo marca como *skipped*. Cada ejecución deja un log con estado y tiempos en `evidence/pipeline_runs/run_<fecha>.json`.

**Idempotencia** (ejecutarlo dos veces no duplica ni corrompe nada):

- `download` salta los ficheros que ya están en `data/`; las descargas se escriben como `*.part` y solo se renombran al terminar.
- `ingest` salta los ficheros que ya están en HDFS con el mismo tamaño; los nuevos se suben a un nombre temporal oculto y se renombran, así la zona raw nunca contiene un fichero a medias. Al final comprueba que están los 24 ficheros con su tamaño exacto.
- Los pasos de Spark escriben con `mode("overwrite")`.

Cada paso devuelve sus conteos (ficheros, bytes, filas) y quedan en el log de la ejecución, así se puede comprobar que la segunda ejecución da los mismos números. Comprobado hasta ahora para `download` e `ingest`: dos ejecuciones seguidas dan `raw zone OK: 24 files (0 uploaded, 24 already there)` (`evidence/pipeline_runs/`); los pasos de Spark se añadirán a esta prueba con la ejecución final.

## Reproducir los experimentos (bloques A, B, C y F)

Con el clúster arrancado y el pipeline ejecutado al menos hasta `ingest`. Todos los tiempos del informe se midieron en el portátil de referencia, uno detrás de otro, sin otros trabajos pesados en marcha.

```bash
# 0. Tras arrancar, esperar a que HDFS salga de safe mode
docker compose exec namenode hdfs dfsadmin -safemode wait

# B. Evidencias de HDFS: zonas, tamaños y bloques
docker compose exec namenode hdfs dfs -ls -R /airline
docker compose exec namenode hdfs dfs -du -s -h /airline/raw/flights /airline/curated/flights
docker compose exec namenode hdfs fsck /airline/raw/flights/2007.csv -files -blocks -locations

# C. Hadoop Streaming (scripts en mapreduce/; el NameNode ya trae python3)
#    un año con y sin combiner, y los 22 años
docker compose exec -T namenode bash -c "bash /opt/mapreduce/run_streaming.sh /airline/raw/flights/1987.csv /airline/results/mr_sample_comb yes local 2>&1" > evidence/miembro3/streaming_sample_comb.log
docker compose exec -T namenode bash -c "bash /opt/mapreduce/run_streaming.sh /airline/raw/flights/1987.csv /airline/results/mr_sample_nocomb no local 2>&1" > evidence/miembro3/streaming_sample_nocomb.log
docker compose exec -T namenode bash -c "bash /opt/mapreduce/run_streaming.sh /airline/raw/flights /airline/results/mr_delay_by_carrier_year yes local 2>&1" > evidence/miembro3/streaming_full.log

# A (5V, diagrama) y C (versión RDD, validación MR = RDD, comparativa):
#    ejecuta las celdas 0–41 del notebook y guarda las salidas en él y en evidence/miembro3/
docker compose exec jupyter python work/scripts/run_notebook.py work/miembro3_mapreduce_reto.ipynb 0-41

# F. Reto de escalado (necesita la zona curated: ejecutar después del paso etl del pipeline)
docker compose exec jupyter python work/scripts/run_notebook.py work/miembro3_mapreduce_reto.ipynb 0-12,22,43-49
```

En PowerShell, cambiar `> fichero` por `| Out-File -Encoding utf8 fichero`.

Antes de lanzar el pipeline o un notebook, comprobar en http://localhost:8080 que no hay otra aplicación de Spark ocupando los 4 cores (un kernel de Jupyter abierto, por ejemplo); si la hay, `docker compose restart jupyter`.

### Resultados esperados (dataset completo)

| Qué | Valor |
|---|---|
| Zona raw | 22 CSV + 2 tablas auxiliares, 10,8 GiB (21,6 GiB con replicación 2) |
| Filas de vuelos | 118.914.458 |
| `fsck` de `2007.csv` | 6 bloques, replicación 2, HEALTHY |
| Hadoop Streaming, 22 años | 295 pares (año, aerolínea), ~175 s |
| Spark RDD, 22 años | mismos 295 pares y mismos conteos que Hadoop, ~97 s |

## Configuración

Todas las rutas y parámetros del pipeline están en **`config/pipeline.yaml`**; los scripts de `scripts/` y `spark/` los leen de ahí (`cfg`) en vez de escribirlos en el código.

```yaml
zones:
  raw_flights: /airline/raw/flights      # CSV originales, sin tocar
  raw_lookup:  /airline/raw/lookup       # airports.csv, carriers.csv
  curated:     /airline/curated/flights  # Parquet limpio, particionado por Year
  results:     /airline/results          # resultados pequeños (CSV permitido)
```

También guarda las reglas comunes a todos los workstreams: vuelo válido (`Cancelled = 0 AND Diverted = 0 AND ArrDelay IS NOT NULL`), retrasado (`ArrDelay > 15`) y mínimo de 100 vuelos por grupo.

## Datos

- 22 ficheros anuales, 1987–2008 (`2008.csv` ocupa 234 MB frente a 500–700 MB del resto de años: el fichero publicado no cubre el año completo).
- Zona raw en HDFS: **10,8 GiB** de datos, **21,6 GiB** en disco por la replicación 2.
- `data/` está en `.gitignore`: los datos nunca se suben a GitHub; se reproducen con `download`.

## Evidencias (workstream 1)

| Fichero | Muestra |
|---|---|
| `evidence/spark_master_2_workers.png` | Spark Master con 2 workers ALIVE (4 cores, 4 GB) |
| `evidence/fsck_2007.txt` | `hdfs fsck` de `2007.csv`: 703 MB → 6 bloques (5 × 128 MB + 31,8 MB), cada uno en los 2 DataNodes, estado HEALTHY |
| `evidence/pipeline_runs/` | Dos ejecuciones seguidas del pipeline con el mismo resultado |

## Estructura del repositorio

```
docker-compose.yml     clúster completo (HDFS + Spark + Jupyter)
config/pipeline.yaml   rutas y parámetros
scripts/               descarga, ingesta y punto de entrada del pipeline
spark/                 ETL, calidad, consultas y ML (workstream 2)
mapreduce/             Hadoop Streaming: mapper, combiner, reducer (workstream 3)
evidence/              capturas, fsck, tiempos, logs
data/                  CSV locales (no se suben)
```

## Git

Antes de cada push: `git pull --rebase`. Commits pequeños y sin datos.
