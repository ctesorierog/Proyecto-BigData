# Aporte de Felipe al informe: Spark y ML

Este texto resume las decisiones y deja identificadas las cifras que deben copiarse de la ejecución final en el portátil de Carlos. Los huecos no son resultados medidos.

## ETL y calidad

Leemos cada CSV anual con un esquema explícito de 29 columnas. Conservamos los nombres originales (`Year`, `UniqueCarrier`, `ArrDelay`, etc.) y seleccionamos las columnas necesarias para las consultas y el modelo. `TailNum` no se usa: en 2001–2002 contiene bytes que no son UTF-8, pero los campos necesarios se pudieron interpretar. Los CSV de raw permanecen intactos. Escribimos curated en Parquet particionado por `Year`.

Un vuelo válido cumple `Cancelled = 0`, `Diverted = 0` y `ArrDelay` disponible. Además, cada salida tiene su propia marca de aptitud. Por ejemplo, un vuelo sin `Distance` puede servir para una consulta, pero no para ML. El paso `quality` cuenta por año estados, nulos, rangos y filas aptas; no confunde un vuelo cancelado con un registro corrupto.

## Tres consultas

1. **Aerolínea y año.** `groupBy(Year, UniqueCarrier)` calcula número de vuelos y media de `ArrDelay`. Conservamos grupos con al menos 100 vuelos.
2. **Tres rutas por origen.** `groupBy(Origin, Dest)` calcula la media de cada ruta. Después de exigir 100 vuelos, `row_number()` ordena las rutas de cada origen por retraso medio descendente y deja las tres primeras. Una media negativa significa que esos vuelos llegaron antes de la hora prevista, no que el cálculo falló.
3. **Aerolínea frente a aeropuerto de origen.** Agrupamos por `Origin` y `UniqueCarrier`, y una ventana suma los retrasos y vuelos de todas las aerolíneas del origen. Dividir esas sumas da la media del aeropuerto ponderada por vuelos. Restar esa media a la de cada aerolínea produce `diferencia_minutos`. El umbral de 100 vuelos se aplica a cada aerolínea después de calcular la media del origen.

El notebook exploratorio ya ejecutó las tres consultas sobre los años 1987–2008; la primera obtuvo 295 grupos aerolínea-año. Confirmar el número final en el resultado del pipeline antes de usarlo en el informe.

## Plan y optimización de la consulta 1

Comparamos el CSV original de 2007 con curated Parquet filtrado a `Year = 2007`, ejecutando la misma consulta y comprobando que ambos resultados coincidan. Medimos el tiempo de `collect()` en el orden CSV, Parquet, Parquet, CSV y usamos la mediana de cada formato. También comparamos bytes lógicos y físicos de HDFS.

En `explain("formatted")` hay que señalar la lectura `FileScan`, el filtro de partición `Year = 2007` en Parquet y los nodos `Exchange` que producen shuffle. En la Spark UI hay que anotar los IDs de etapas y `Shuffle Read/Write` de las ejecuciones etiquetadas `consulta1_2007_*`, y guardar capturas. El notebook `02_integracion_carlos.ipynb` genera `comparacion.json`, `plan_csv.txt` y `plan_parquet.txt` en `spark/output/benchmark_2007/`.

| Medida final en el mismo portátil | CSV | Parquet |
|---|---:|---:|
| Tamaño lógico en HDFS (MiB) | pendiente | pendiente |
| Mediana de tiempo (s) | pendiente | pendiente |
| IDs de etapas y shuffle (Spark UI) | pendiente | pendiente |

La diferencia de tamaño refleja tanto el formato como la selección de columnas de curated; no atribuirla exclusivamente a Parquet.

## Línea base MLlib

La etiqueta es `ArrDelay > 15`. Entrenamos una regresión logística con una muestra reproducible del 2 % de 2006 y evaluamos en 2007. Usamos solo datos conocidos antes de despegar: mes, día de semana, hora programada, aerolínea, origen, destino y distancia. Las variables categóricas se indexan y codifican; MLlib ensambla los predictores y calcula AUC ROC y accuracy. Evitamos 2008 porque el archivo publicado es parcial.

| Métrica real guardada por el pipeline en `results/spark/ml_baseline` | Valor |
|---|---:|
| Filas de entrenamiento / prueba | pendiente |
| Proporción de vuelos retrasados en prueba | pendiente |
| AUC ROC / accuracy | pendiente |

**Limitación:** los contenedores comparten un único ordenador. Los tiempos del CSV y Parquet deben medirse en el mismo portátil de referencia, sin otros trabajos pesados en marcha; las pruebas con datos inventados verifican el código, pero no son resultados del proyecto.
