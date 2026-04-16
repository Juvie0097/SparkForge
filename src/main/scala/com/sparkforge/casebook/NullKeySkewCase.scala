package com.sparkforge.casebook

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Case: NULL join keys collapse onto one partition.
 *
 * Symptom in Spark UI:
 *   - One reducer task takes 10–100× longer than the median
 *   - Shuffle read on that task is enormous
 *   - GC time on the task spikes; eventually OOM
 *
 * Cause: hash partitioner sends all NULLs to the same reducer. SQL semantics
 * say NULL != NULL, so those rows wouldn't match anything anyway — they just
 * pile up and waste a task.
 *
 * Fix options:
 *   A. Filter NULLs out before the join.
 *   B. Replace NULL with a "synthetic non-matching" sentinel that hashes
 *      across partitions, then drop the result of those rows after the join
 *      (cheapest when nulls must survive in output).
 */
object NullKeySkewCase {

  /** Reproduce the problem. The join here is a left join: NULLs come through
   *  but cause skew. */
  def buggy(facts: DataFrame, dim: DataFrame, key: String = "device_id"): DataFrame =
    facts.join(dim, Seq(key), "left")

  /** Fix A — filter. Use when NULL rows can be dropped. */
  def fixedFilterNulls(facts: DataFrame, dim: DataFrame, key: String = "device_id"): DataFrame = {
    val (notNull, _) = (facts.filter(col(key).isNotNull),
                        facts.filter(col(key).isNull))
    notNull.join(dim, Seq(key), "left")
  }

  /** Fix B — sentinel. Use when NULL rows must survive (typical for left join). */
  def fixedSentinel(facts: DataFrame, dim: DataFrame, key: String = "device_id"): DataFrame = {
    // Add a per-row salt so NULLs spread across partitions but still don't
    // match any dim row (sentinel keys are guaranteed unique).
    val sentinelFacts = facts.withColumn(key,
      when(col(key).isNull,
           concat(lit("__null_sentinel_"), monotonically_increasing_id().cast("string")))
        .otherwise(col(key)))
    sentinelFacts.join(dim, Seq(key), "left")
      // Optionally normalize sentinels back to NULL in the output:
      .withColumn(key,
        when(col(key).startsWith("__null_sentinel_"), lit(null).cast("string"))
          .otherwise(col(key)))
  }

  /** Demo entry — run all three side by side and print row counts. */
  def demo(spark: SparkSession, factsPath: String, dimPath: String): Unit = {
    val f = spark.read.parquet(factsPath)
    val d = spark.read.parquet(dimPath)
    Seq(
      "buggy"           -> buggy(f, d),
      "fix-filter"      -> fixedFilterNulls(f, d),
      "fix-sentinel"    -> fixedSentinel(f, d)
    ).foreach { case (label, df) =>
      val t0 = System.nanoTime()
      val n = df.count()
      val ms = (System.nanoTime() - t0) / 1e6
      println(f"[NullKeySkew] $label%-15s rows=$n%-12d ms=$ms%.0f")
    }
  }
}
