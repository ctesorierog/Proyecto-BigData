# Proyecto-BigData
## Clúster de pruebas: HDFS + Spark + Jupyter

Entorno de pruebas con Docker Compose que levanta un clúster **HDFS** con 2 DataNodes, un clúster **Spark standalone** con 2 workers y un **Jupyter** con PySpark conectado a ambos. Está pensado para trabajar con los notebooks de `~/spark-course` y con el dataset de vuelos de `~/zipmuygrande` (Airline On-Time, 1987–2008, ~11,5 GB).

### Arquitectura

| Servicio | Imagen | Función |
|---|---|---|
| `namenode` | `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` | Maestro de HDFS: guarda los metadatos (qué bloques forman cada fichero y en qué DataNode están). Monta `~/zipmuygrande` en `/data` (solo lectura) para subir los CSV. |
| `datanode1`, `datanode2` | `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` | Guardan los bloques de datos. Con `dfs.replication=2` cada bloque tiene una copia en cada DataNode. |
| `spark-master` | `quay.io/jupyter/pyspark-notebook` | Maestro de Spark standalone: reparte el trabajo entre los workers. |
| `spark-worker1`, `spark-worker2` | `quay.io/jupyter/pyspark-notebook` | Ejecutores de Spark, con **2 cores y 2 GB** cada uno. Montan `~/spark-course` en la misma ruta que Jupyter, así que las rutas relativas (`data/sales`) funcionan. |
| `jupyter` | `quay.io/jupyter/pyspark-notebook` | Driver de PySpark. Monta `~/spark-course` en `/home/jovyan/work`. |

#### Decisiones de diseño

- **Misma imagen para Jupyter y Spark.** El driver (Jupyter) y los executors tienen que tener la misma versión de Spark (4.x) y de Python. Por eso los workers no usan las imágenes `bde2020/spark-*`, que traen Spark 3.3 y Python 3.7.
- **2 DataNodes.** Es el mínimo para que haya replicación y tolerancia a fallos. En una sola máquina, más DataNodes no aceleran nada (todos comparten el mismo disco) y consumen RAM.
- **2 workers × 2 cores × 2 GB.** Pensado para un equipo de 8 cores y 16 GB de RAM: deja recursos libres para HDFS, el driver y el sistema operativo.
- **Sin YARN.** Spark usa su propio planificador (standalone). Así se ahorran ~2 GB de RAM.
- **`PYSPARK_SUBMIT_ARGS`** en Jupyter hace que `SparkSession.builder.getOrCreate()` se conecte al clúster (`spark://spark-master:7077`) en lugar de ejecutarse en `local[*]`.
- **`HADOOP_USER_NAME=root`** permite escribir en HDFS sin problemas de permisos (atajo de laboratorio, no para producción).

### Puesta en marcha

Hay que parar antes el resto de contenedores que usan los mismos puertos:

```bash
docker stop spark-course                 # Jupyter antiguo (8888, 4040)
(cd .. && docker compose down)           # clúster Hadoop principal (9870, 9000)
```

Arrancar el clúster:

```bash
cd pruebas
docker compose up -d
docker compose ps                        # los 6 contenedores deben estar "Up"
docker logs pruebas-jupyter 2>&1 | grep token   # enlace de Jupyter con token
```

Parar el clúster:

```bash
docker compose down        # conserva los datos de HDFS
docker compose down -v     # borra también los volúmenes de HDFS
```

### Enlaces

| Interfaz | URL | Qué se ve |
|---|---|---|
| Jupyter | http://localhost:8888 | Notebooks de `spark-course` (carpeta `work`) |
| HDFS NameNode | http://localhost:9870 | Estado de HDFS, DataNodes vivos, explorador de ficheros (*Utilities → Browse the file system*) |
| Spark Master | http://localhost:8080 | Workers registrados y aplicaciones en ejecución |
| Spark Worker 1 | http://localhost:8081 | Executors del worker 1 |
| Spark Worker 2 | http://localhost:8082 | Executors del worker 2 |
| Spark Application UI | http://localhost:4040 | Jobs, stages y tareas de la sesión activa (solo con una SparkSession abierta) |

### Cargar los datos en HDFS

```bash
docker exec pruebas-namenode hdfs dfs -mkdir -p /airline/flights /airline/lookup
docker exec pruebas-namenode bash -c 'hdfs dfs -put -f /data/19*.csv /data/20*.csv /airline/flights/'
docker exec pruebas-namenode bash -c 'hdfs dfs -put -f /data/airports.csv /data/carriers.csv /data/plane-data.csv /data/variable-descriptions.csv /airline/lookup/'
docker exec pruebas-namenode hdfs dfs -du -h /airline
```

### Uso desde un notebook

```python
import os
os.environ["HADOOP_USER_NAME"] = "root"

from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.appName("vuelos").getOrCreate()   # se conecta al clúster

HDFS = "hdfs://namenode:9000"
flights = (spark.read.option("header", True).option("inferSchema", True)
                .csv(f"{HDFS}/airline/flights"))
airports = spark.read.option("header", True).csv(f"{HDFS}/airline/lookup/airports.csv")

# Opcional: convertir a Parquet una vez para que las consultas sean mucho más rápidas
flights.write.mode("overwrite").partitionBy("Year").parquet(f"{HDFS}/airline/flights_parquet")
```

Notas:

- Los notebooks que usan `.master("local[*]")` se ejecutan solo dentro del contenedor de Jupyter. Para usar el clúster, quita esa línea.
- Si un kernel estaba abierto antes de arrancar el clúster, reinícialo (*Kernel → Restart*).
