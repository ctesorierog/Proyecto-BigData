#!/bin/bash
# Uso: bash run_streaming.sh <entrada_hdfs> <salida_hdfs> [yes|no: combiner] [local|yarn]
set -eo pipefail
INPUT=${1:-/airline/raw/flights}
OUTPUT=${2:-/airline/results/mr_delay_by_carrier_year}
USE_COMBINER=${3:-yes}
MODE=${4:-local}
DIR=/opt/mapreduce
JAR=$(ls "$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-streaming-*.jar | head -1)

hdfs dfs -rm -r -f -skipTrash "$OUTPUT"            # idempotente: relanzar no falla ni duplica

if [ "$MODE" = "yarn" ]; then                       # tareas en el NodeManager: enviamos los scripts
  GENERIC=(-D mapreduce.framework.name=yarn -files "$DIR/mapper.py,$DIR/combiner.py,$DIR/reducer.py")
  MAP="python3 mapper.py"; COMB="python3 combiner.py"; RED="python3 reducer.py"
else                                                # sin YARN: 4 hilos de map, como los 4 cores de Spark
  GENERIC=(-D mapreduce.framework.name=local -D mapreduce.local.map.tasks.maximum=4)
  MAP="python3 $DIR/mapper.py"; COMB="python3 $DIR/combiner.py"; RED="python3 $DIR/reducer.py"
fi
COMBINER=()
if [ "$USE_COMBINER" = "yes" ]; then COMBINER=(-combiner "$COMB"); fi

echo "INPUT=$INPUT OUTPUT=$OUTPUT COMBINER=$USE_COMBINER MODE=$MODE"
START=$(date +%s.%N)
hadoop jar "$JAR" "${GENERIC[@]}" \
  -D mapreduce.job.reduces=1 \
  -D mapreduce.job.name="retraso_por_anio_aerolinea" \
  -mapper "$MAP" "${COMBINER[@]}" -reducer "$RED" \
  -input "$INPUT" -output "$OUTPUT"
END=$(date +%s.%N)
echo "ELAPSED_SECONDS=$(awk "BEGIN{print $END - $START}")"
hdfs dfs -cat "$OUTPUT/part-*" | head -5
