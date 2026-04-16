package com.sparkforge.join

import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Centralised join strategy primitives. Keeping each one named explicitly
 * makes performance comparisons in `TUNING_NOTES.md` reproducible — every
 * benchmark in the casebook calls back into here.
 *
 * Decision matrix (in practice):
 *   - Small dim (< autoBroadcastJoinThreshold, default 10MB)  → BHJ
 *   - Medium dim, fits in executor mem                        → SHJ (rare)
 *   - Large fact x large fact                                  → SMJ
 *   - Both sides bucketed on join key with same #buckets       → Bucket Join
 *
 * Catalyst auto-picks BHJ when stats say it's safe; we hint anyway because
 * stats refresh isn't always reliable on HDFS Parquet without ANALYZE.
 */
object JoinStrategies {

  /** Force a broadcast hash join. Throws at planning time if right side is huge. */
  def broadcastJoin(left: DataFrame, right: DataFrame, key: String): DataFrame =
    left.join(broadcast(right), Seq(key), "left")

  /** Default sort-merge join — relied upon when both sides are large. */
  def sortMergeJoin(left: DataFrame, right: DataFrame, key: String): DataFrame = {
    // Use the merge hint so AQE doesn't silently downgrade to BHJ if the
    // right side ends up small after filters.
    left.hint("merge").join(right, Seq(key), "left")
  }

  /** Shuffle hash join — useful when right side is mid-sized & sorted is overkill. */
  def shuffleHashJoin(left: DataFrame, right: DataFrame, key: String): DataFrame =
    left.join(right.hint("shuffle_hash"), Seq(key), "left")

  /**
   * Wrapper that guards against the most common implicit-cast trap: if the
   * key types don't match, Spark silently casts and breaks predicate pushdown.
   * Crash early instead.
   */
  def safeJoin(left: DataFrame, right: DataFrame, key: String, hint: String = "auto"): DataFrame = {
    val lt = left.schema(key).dataType
    val rt = right.schema(key).dataType
    require(lt == rt, s"Join key '$key' type mismatch: $lt vs $rt — fix before joining")
    hint match {
      case "broadcast"    => broadcastJoin(left, right, key)
      case "merge"        => sortMergeJoin(left, right, key)
      case "shuffle_hash" => shuffleHashJoin(left, right, key)
      case _              => left.join(right, Seq(key), "left")
    }
  }

  /**
   * Demonstration helper: run the same join three different ways and report
   * #stages and shuffle bytes. Used by the casebook + tuning notes.
   */
  def benchmarkJoins(spark: SparkSession, left: DataFrame, right: DataFrame, key: String): Unit = {
    val plans = Map(
      "broadcast"    -> broadcastJoin   (left, right, key),
      "merge"        -> sortMergeJoin   (left, right, key),
      "shuffle_hash" -> shuffleHashJoin (left, right, key)
    )
    plans.foreach { case (name, df) =>
      // count() materialises the plan → triggers actual shuffle reads/writes
      val t0 = System.nanoTime()
      val rows = df.count()
      val ms = (System.nanoTime() - t0) / 1e6
      println(f"[bench] strategy=$name%-13s rows=$rows%-12d duration_ms=$ms%.0f")
    }
  }
}
