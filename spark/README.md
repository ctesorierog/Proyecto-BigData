# Etapas Spark de Felipe

Estos módulos encajan con `scripts/run_pipeline.py`: cada uno expone
`run(spark, cfg)` y lee las rutas de `config/pipeline.yaml`.

Con los 22 CSV anuales y las dos tablas auxiliares ya cargados en HDFS:

```bash
docker compose exec jupyter python work/scripts/run_pipeline.py --steps etl quality analysis ml
```

- `etl.py`: lee los vuelos con esquema explícito, añade marcas de aptitud y escribe Parquet particionado por `Year` en curated.
- `quality.py`: registra por año nulos, rangos, estados y filas aptas para cada salida; guarda el informe en results. Los cancelados y desviados se cuentan sin tratarlos como corrupción. Este paso no hace una búsqueda completa de duplicados; `evidence/miembro3/5v_veracidad.json` contiene el análisis separado de claves duplicadas.
- `analysis.py`: escribe las tres consultas en results con el mínimo de grupo del YAML.
- `ml_baseline.py`: entrena una regresión logística con muestra reproducible de 2006 y evalúa en 2007; guarda AUC ROC, accuracy y número de filas en results.

El notebook `notebooks/02_integracion_carlos.ipynb` contiene la comparación de la consulta 1 entre CSV y Parquet de 2007. Tras ejecutar ETL, las celdas de código 2, 4, 6, 18, 20 y 22 preparan el benchmark, miden tamaño y tiempo y guardan los planes `explain()`. Para evidenciar etapas y shuffles, ejecuta el notebook de forma interactiva y captura la Spark UI mientras la sesión siga abierta.

Las mediciones finales, el resultado real de ML y la doble ejecución del pipeline completo deben hacerse en el mismo portátil de referencia. Los ensayos locales con datos inventados solo comprueban que el código funciona; no son resultados del proyecto.

`SECCION_PARA_INFORME.md` explica las tres consultas, el benchmark y ML en lenguaje de informe, con espacios claramente marcados para las medidas reales que faltan.
