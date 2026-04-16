#!/usr/bin/env bash
# spark-submit template for SparkForge.
#
# Usage:
#   scripts/submit.sh gen
#   scripts/submit.sh features baseline
#   scripts/submit.sh case nullkey
#
# All overrides go through `--conf` so the application code stays unchanged
# between local laptop and YARN/K8s.
set -euo pipefail

STAGE="${1:-features}"
ARG="${2:-recommended}"

JAR="${JAR:-target/scala-2.12/sparkforge-0.1.0.jar}"
MASTER="${MASTER:-yarn}"
DEPLOY_MODE="${DEPLOY_MODE:-cluster}"

# ---- resource sizing ---------------------------------------------------
# Sweet spot: 4-5 cores per executor; more = GC contention, less = JVM overhead.
EXECUTORS="${EXECUTORS:-20}"
EXECUTOR_CORES="${EXECUTOR_CORES:-4}"
EXECUTOR_MEM="${EXECUTOR_MEM:-8g}"
EXECUTOR_OVERHEAD="${EXECUTOR_OVERHEAD:-2g}"   # max(384m, 10% of executor.memory)
DRIVER_MEM="${DRIVER_MEM:-4g}"

# ---- common conf -------------------------------------------------------
COMMON_CONF=(
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
  --conf spark.sql.session.timeZone=UTC

  # AQE — also enabled in code, but the conf survives even if app forgets.
  --conf spark.sql.adaptive.enabled=true
  --conf spark.sql.adaptive.coalescePartitions.enabled=true
  --conf spark.sql.adaptive.skewJoin.enabled=true
  --conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128MB

  # Shuffle / files
  --conf spark.sql.shuffle.partitions=400
  --conf spark.sql.files.maxPartitionBytes=128MB
  --conf spark.sql.files.openCostInBytes=4MB
  --conf spark.sql.files.maxRecordsPerFile=5000000

  # Joins
  --conf spark.sql.autoBroadcastJoinThreshold=32MB
  --conf spark.sql.adaptive.autoBroadcastJoinThreshold=32MB

  # DPP & runtime bloom (Spark 3.3+)
  --conf spark.sql.optimizer.dynamicPartitionPruning.enabled=true
  --conf spark.sql.optimizer.runtime.bloomFilter.enabled=true

  # Dynamic allocation (needs External Shuffle Service on workers)
  --conf spark.dynamicAllocation.enabled=true
  --conf spark.dynamicAllocation.minExecutors=4
  --conf spark.dynamicAllocation.maxExecutors=80
  --conf spark.dynamicAllocation.executorIdleTimeout=120s
  --conf spark.shuffle.service.enabled=true

  # Speculation — kept ON for read-heavy stages, but the writer modules
  # disable it locally to stay idempotent.
  --conf spark.speculation=true
  --conf spark.speculation.multiplier=1.5
  --conf spark.speculation.quantile=0.75

  # Memory / GC
  --conf spark.memory.fraction=0.6
  --conf spark.memory.storageFraction=0.4
  --conf spark.executor.extraJavaOptions="-XX:+UseG1GC -XX:+UseStringDeduplication"
  --conf spark.driver.extraJavaOptions="-XX:+UseG1GC"

  # Event log for History Server
  --conf spark.eventLog.enabled=true
  --conf spark.eventLog.dir=hdfs:///spark2-history/

  # Multi-disk shuffle
  --conf spark.local.dir=/data1/spark,/data2/spark
)

spark-submit \
  --master "$MASTER" \
  --deploy-mode "$DEPLOY_MODE" \
  --num-executors "$EXECUTORS" \
  --executor-cores "$EXECUTOR_CORES" \
  --executor-memory "$EXECUTOR_MEM" \
  --conf spark.executor.memoryOverhead="$EXECUTOR_OVERHEAD" \
  --driver-memory "$DRIVER_MEM" \
  --class com.sparkforge.SparkForgeApp \
  "${COMMON_CONF[@]}" \
  "$JAR" \
  --stage "$STAGE" --tuning recommended ${ARG:+--case "$ARG"}
