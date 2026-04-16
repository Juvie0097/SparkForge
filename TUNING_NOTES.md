# Tuning Notes

A worksheet to fill in as you run the experiments. Numbers are placeholders —
overwrite with your own measurements. The point of the project is *not* to
prove a particular setting is best, but to give you a reproducible harness in
which to find out.

How to read each table:

- **Wall-clock**: Stage end-to-end time from `MetricsCollector.report()`.
- **Shuffle MB**: total shuffle read bytes for the heaviest stage.
- **Skew (max/median)**: > 5 means a long-tail problem.
- **# Output files**: from `FileSizeAnalyzer.analyze`.

---

## 1. Baseline tuning experiments

### 1.1 AQE on / off (feature pipeline)

| Variant                          | shuffle.partitions | wall-clock | shuffle MB | skew (max/median) | notes |
|----------------------------------|--------------------|-----------|-----------|-------------------|-------|
| baseline (AQE off, 200 parts)    | 200                |           |           |                   |       |
| AQE on, 200 init parts           | 200 → AQE coalesce |           |           |                   |       |
| AQE on, 1000 init parts          | 1000 → AQE coalesce|           |           |                   |       |
| AQE on, advisoryPartition=64MB   | dynamic            |           |           |                   |       |

Expected pattern: AQE coalesce drops final partitions to ~`totalShuffleBytes /
advisorySize`; wall-clock drops accordingly when the baseline was over-
partitioned.

### 1.2 Join strategy (fact × dim_merchants, ~5k rows)

| Strategy        | wall-clock | shuffle MB | plan              | when to use        |
|-----------------|-----------|-----------|-------------------|--------------------|
| BroadcastHash   |           | ~0        | BroadcastHashJoin | dim < 32 MB        |
| SortMerge       |           |           | SortMergeJoin     | both sides large   |
| ShuffleHash     |           |           | ShuffledHashJoin  | mid-size, sorted not needed |
| BucketedJoin    |           | ~0 (no Exchange) | SMJ over bucketed | repeat queries on stable schema |

### 1.3 Cache level (enriched fact, used by 3 feature aspects)

| Level                | wall-clock (3 aspects total) | storage MB | GC time | notes |
|----------------------|------------------------------|-----------|---------|-------|
| no cache             |                              |   0       |         | recomputes everything |
| MEMORY_ONLY          |                              |           |         | OOM-prone on wide rows |
| MEMORY_AND_DISK_SER  |                              |           |         | recommended default   |
| DISK_ONLY            |                              |           |         | fallback for huge wide tables |
| checkpoint (truncate)|                              |           |         | shorter plan, slow first time |

---

## 2. Skew handling experiments

Setup: `hot-user-fraction = 0.30`, `hot-user-count = 100`. Run
`--stage case --case twostage` and compare to plain `--stage features`.

| Approach                 | wall-clock | max task ms | median task ms | shuffle MB | notes |
|--------------------------|-----------|-------------|----------------|-----------|-------|
| naive groupBy            |           |             |                |           |       |
| AQE skewJoin (SMJ only)  |           |             |                |           | only helps joins, not groupBy |
| salting (16 buckets)     |           |             |                |           |       |
| hot/cold split           |           |             |                |           | best when hot keys < few hundred |
| two-stage aggregation    |           |             |                |           | required for groupBy skew |

---

## 3. Small-file experiments

### 3.1 Write-side
| Variant                                          | files written | avg MB | small ratio | wall-clock |
|--------------------------------------------------|---------------|--------|-------------|-----------|
| `repartition(2000)` then write                   |               |        |             |           |
| AQE coalesce + advisorySize=128MB                |               |        |             |           |
| `repartition(partitionCols)` then write          |               |        |             |           |
| `repartition(partitionCols, salt)` × 4           |               |        |             |           |

### 3.2 Read-side
| Variant                              | input partitions | wall-clock | notes |
|--------------------------------------|------------------|-----------|-------|
| 2000 small files, default reader     |                  |           |       |
| 2000 small files, openCostInBytes=8MB|                  |           |       |
| 2000 small files, maxPartitionBytes=256MB |             |           |       |

### 3.3 Compaction outcome
| Path                          | before files | after files | before avg MB | after avg MB |
|-------------------------------|--------------|-------------|---------------|---------------|
| /raw/fact_transactions        |              |             |               |               |
| /raw/dim_users                |              |             |               |               |

---

## 4. Casebook results

| Case                | buggy ms | fixed ms | observation                      |
|---------------------|---------|---------|----------------------------------|
| NullKeySkewCase     |         |         | one task >> others in buggy      |
| ImplicitCastJoinCase|         |         | PushedFilters empty in buggy     |
| TwoStageAggCase     |         |         | even task durations in fixed     |
| UdfPushdownCase     |         |         | "PushedFilters: []" in buggy plan|
| WindowSkewCase      |         |         | spill-to-disk drops in fixed     |
| DynamicPartitionWrite|        |         | file count drops 100×            |

---

## 5. Memory & GC notes (collect after each run)

| Run                       | executor heap | GC % | spill memory | spill disk |
|---------------------------|---------------|------|--------------|------------|
| baseline                  |               |      |              |            |
| AQE on                    |               |      |              |            |
| with cache wide table     |               |      |              |            |
| off-heap memory enabled   |               |      |              |            |

---

## 6. Things to look for in Spark UI

- **Stages → "Aggregated Metrics by Executor"**: spill-to-disk numbers.
  Non-zero spill at every stage means `spark.memory.fraction` is too low or
  the operation is genuinely too large.
- **SQL → Query plan tree**: look for `AQEShuffleRead` (AQE active),
  `OptimizedSkewJoin` (skew-join split happened),
  `BroadcastHashJoin (runtime promotion)` (BHJ promoted at runtime).
- **Storage**: confirm that the things you `cache()`'d are actually in memory,
  not silently evicted.
- **Executors → Task Time / GC Time**: > 10% GC indicates serious memory
  pressure; switch to `MEMORY_AND_DISK_SER`, increase off-heap, or shrink the
  cached set.

---

## 7. Tools-of-trade reminders

- `df.queryExecution.optimizedPlan.stats.sizeInBytes` — Catalyst's idea of
  the row group's size; useful when deciding between BHJ and SMJ.
- `spark.conf.getAll` filtered by `adaptive`, `shuffle`, `files` — confirms
  no env-level override broke your AQE settings.
- `df.inputFiles.length` — quick "how many files am I about to read" check.
