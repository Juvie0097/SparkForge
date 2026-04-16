package com.sparkforge.casebook

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Case: groupBy on a heavily-skewed key (a few users own 30% of rows).
 *
 * Symptom:
 *   - One reducer at 100% CPU for minutes while others are idle
 *   - Long-tail Stage; AQE skewJoin doesn't help (no join here)
 *
 * Cause: shuffle-by-key plus aggregation puts every row of the hot key on a
 * single reducer.
 *
 * Fix: two-stage aggregation
 *   Stage 1 (local): groupBy(key, salt) → partial aggregate. The salt spreads
 *                    the hot key across `salt` reducers.
 *   Stage 2 (final): groupBy(key)        → merge the partial aggregates.
 *
 * Works because most aggregations are *associative* (sum, count, min, max,
 * approx_*). For non-associative aggs (e.g. `collect_list` order-preserving),
 * this trick doesn't apply — re-think the requirement.
 */
object TwoStageAggCase {

  /** Naive — single shuffle, one reducer per key. */
  def buggy(txns: DataFrame): DataFrame =
    txns.groupBy("user_id").agg(
      count(lit(1)).alias("cnt"),
      sum("amount").alias("amt_sum")
    )

  /** Two-stage. `salt` controls hot-key spread (16 is a safe default). */
  def fixed(txns: DataFrame, salt: Int = 16): DataFrame = {
    val partial = txns
      .withColumn("__salt", (rand() * lit(salt)).cast("int"))
      .groupBy("user_id", "__salt")
      .agg(
        count(lit(1)).alias("cnt_part"),
        sum("amount").alias("amt_part")
      )
    partial
      .groupBy("user_id")
      .agg(
        sum("cnt_part").alias("cnt"),
        sum("amt_part").alias("amt_sum")
      )
  }

  def demo(spark: SparkSession, factsPath: String): Unit = {
    val f = spark.read.parquet(factsPath)
    Seq("buggy" -> buggy(f), "fixed" -> fixed(f)).foreach { case (label, df) =>
      val t0 = System.nanoTime()
      val n = df.count()
      val ms = (System.nanoTime() - t0) / 1e6
      println(f"[TwoStageAgg] $label%-6s rows=$n%-12d ms=$ms%.0f")
    }
  }
}
