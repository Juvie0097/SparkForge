# Casebook — Real-world Spark traps

Each file here follows a 3-step structure: **buggy → symptom → fixed**, so the
reader can `git diff buggy fixed` to see the change in isolation. None of the
fixes are exotic; they are all things that come up in code review repeatedly.

| File | Trap | Fix |
|---|---|---|
| `NullKeySkewCase.scala`         | NULL join keys piled on one reducer            | filter, or sentinel-replace |
| `ImplicitCastJoinCase.scala`    | type mismatch on join key kills pushdown       | align types eagerly |
| `BroadcastTimeoutCase.scala`    | `broadcast()` on a not-actually-small dim      | tighten threshold or explicit broadcast var |
| `TwoStageAggCase.scala`         | groupBy on hot key → stage long-tail           | salt + partial agg + re-agg |
| `SpeculationCase.scala`         | straggler tasks; speculation pitfalls          | enable carefully, disable for non-idempotent writers |
| `UdfPushdownCase.scala`         | UDF in WHERE blocks Parquet pushdown           | rewrite with built-in / `expr` |
| `CodegenLimitCase.scala`        | 64KB whole-stage codegen limit                 | split pipeline / lower hugeMethodLimit |
| `WindowSkewCase.scala`          | `partitionBy(hot_key)` window stuck            | salted top-K or `min_by/max_by` |
| `DynamicPartitionWriteCase.scala`| fan-out write → tiny files & OOM              | `repartition(partitionCols)` before write |

## How to verify a fix actually worked

For each case the demo prints wall-clock and row count. Beyond that, look at
the Spark UI:

1. **Stages tab → Summary Metrics** — compare *Max* vs *Median* task duration.
   A healthy stage has Max < 2× Median; skew shows up as 10–100×.
2. **SQL tab → query plan** — search for `PushedFilters`, `ReusedExchange`,
   `BroadcastHashJoin`, `AQEShuffleRead`, `OptimizedSkewJoin`.
3. **Executors tab → GC Time / Task Time** — > 10% GC suggests cache/storage
   pressure; consider `MEMORY_AND_DISK_SER` or off-heap.
