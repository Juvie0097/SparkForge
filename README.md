# SparkForge

A Scala/Spark project that simulates a financial transaction feature pipeline
and uses it as a sandbox for exercising **performance tuning, skew handling,
small-file mitigation, AQE behaviour, and a catalogue of real-world Spark
traps**.

The project is intentionally hands-on: every module is independently runnable,
every "fix" is paired with the original "buggy" version next to it, and every
benchmark prints both wall-clock and Spark UI–verifiable metrics.

## What's in the box

```
src/main/scala/com/sparkforge/
├── data/             # Schema + skewed transaction generator
├── features/         # Frequency / Amount / Device features over 1/7/30d
├── join/             # Broadcast / SMJ / SHJ + 3 skew-handling strategies
├── tuning/           # AQE configurator + opinionated cache manager
├── compact/          # Small-file analyzer, adaptive coalescer, OPTIMIZE-style job
├── casebook/         # 9 real-world traps, each as buggy-vs-fixed side by side
├── advanced/         # Bucket join, DPP, custom partitioner & Catalyst rule,
│                     # mapPartitions batching, idempotent accumulator
├── util/             # SparkListener-based metrics collector + explain dumper
└── SparkForgeApp.scala
```

## Quick start (laptop)

```bash
# 1. compile
sbt assembly

# 2. generate ~10M skewed transactions + dims
sbt "runMain com.sparkforge.SparkForgeApp --stage gen --tuning laptop"

# 3. run the feature pipeline (1/7/30 day windows, broadcast + SMJ joins)
sbt "runMain com.sparkforge.SparkForgeApp --stage features --tuning laptop"

# 4. inspect the small-file problem and compact
sbt "runMain com.sparkforge.SparkForgeApp --stage analyze"
sbt "runMain com.sparkforge.SparkForgeApp --stage compact"

# 5. play with cases
sbt "runMain com.sparkforge.SparkForgeApp --stage case --case nullkey"
sbt "runMain com.sparkforge.SparkForgeApp --stage case --case twostage"
sbt "runMain com.sparkforge.SparkForgeApp --stage case --case udf"
```

## Cluster

```bash
sbt assembly
JAR=target/scala-2.12/sparkforge-0.1.0.jar scripts/submit.sh features
```

`scripts/submit.sh` documents the resource sizing, AQE knobs, dynamic
allocation, speculation, GC, and shuffle-disk choices line by line.

## Recommended reading order

1. `casebook/README.md` — start here. Each case is a 30-line illustration of
   a problem you'll meet in real Spark code.
2. `tuning/AqeConfigurator.scala` — every knob explained, three preset modes.
3. `compact/CompactJob.scala` — pure-Spark OPTIMIZE; explains the staging-then-rename pattern.
4. `TUNING_NOTES.md` — A/B tables to fill in as you run the experiments.

## Things this project deliberately exercises

- Broadcast / SortMerge / ShuffleHash / Bucket join — all four
- AQE: `enabled`, `coalescePartitions`, `skewJoin`, `localShuffleReader`,
  runtime BHJ promotion, runtime bloom filter
- Skew: AQE auto, salting, hot/cold split, two-stage aggregation
- Small files: write-side `repartition(partitionCols)`, target file sizing,
  read-side `maxPartitionBytes` / `openCostInBytes`, OPTIMIZE-style rewrite
- Wide-table caching strategies (`MEMORY_AND_DISK_SER`, checkpoint to truncate lineage)
- DPP, predicate pushdown, partition pruning, runtime bloom
- Whole-stage codegen 64KB limit + escape hatches
- UDF vs built-in pushdown
- Speculation, idempotency, accumulator double-count
- Dynamic partition write fan-out
- NULL-key skew, implicit-cast pushdown loss, broadcast timeout
- Custom Catalyst rule + custom partitioner
- SparkListener-based per-stage metrics

## Notes on environment

- Built for Spark **3.5.1** + Scala **2.12.18** (Spark 3.5 has the most mature
  AQE / DPP / runtime bloom).
- Local runs need JDK 11+; tests pass `--add-opens` flags via sbt.
- For a real cluster, set `MASTER=yarn` and your event log dir before invoking
  `scripts/submit.sh`.
