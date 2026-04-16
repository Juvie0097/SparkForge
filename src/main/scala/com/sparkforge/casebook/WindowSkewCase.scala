package com.sparkforge.casebook

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Case: window function on a skewed partitionBy column.
 *
 * Symptom: stage stuck on a single task; spill to disk in GB; OOM on hot key.
 *
 * Cause: `Window.partitionBy(user_id)` triggers a shuffle keyed by user_id.
 * For a hot user with millions of rows, *all* of them land on one executor,
 * which then has to sort them in memory (or spill to disk).
 *
 * Fixes:
 *  1. If you only need the top-K per partition, use `dense_rank` + filter
 *     before any other window — small intermediate, AQE skewJoin can help.
 *  2. If you need a strict order, replace the global sort by a
 *     "salted top-K then merge" two-stage pattern.
 *  3. For most "first / last per group" cases, `groupBy + min_by/max_by` is
 *     a much cheaper alternative than a window.
 */
object WindowSkewCase {

  /** Buggy: row_number over the whole user partition then filter. */
  def buggyTopK(txns: DataFrame, k: Int = 5): DataFrame = {
    val w = Window.partitionBy("user_id").orderBy(col("amount").desc)
    txns.withColumn("rn", row_number().over(w))
      .filter(col("rn") <= k)
      .drop("rn")
  }

  /** Fix: pre-aggregate "candidates" via a salted partial top-K, then merge.
   *  For the merge step the per-user input is already bounded by k * salt,
   *  which is tiny. */
  def fixedTwoStageTopK(txns: DataFrame, k: Int = 5, salt: Int = 16): DataFrame = {
    val salted = txns.withColumn("__salt", (rand() * lit(salt)).cast("int"))

    val partial = {
      val w = Window.partitionBy("user_id", "__salt").orderBy(col("amount").desc)
      salted.withColumn("rn", row_number().over(w))
        .filter(col("rn") <= k)
        .drop("rn", "__salt")
    }

    val finalW = Window.partitionBy("user_id").orderBy(col("amount").desc)
    partial.withColumn("rn", row_number().over(finalW))
      .filter(col("rn") <= k)
      .drop("rn")
  }

  /** Fix: replace 'first per user' window with groupBy + min_by. */
  def fixedFirstPerUserGroupBy(txns: DataFrame): DataFrame = {
    // min_by / max_by were added in Spark 3.0; both avoid the window sort.
    txns.groupBy("user_id").agg(
      expr("min_by(struct(*), ts) as first_event"),
      expr("max_by(struct(*), ts) as last_event")
    )
  }

  def demo(spark: SparkSession, factsPath: String): Unit = {
    val f = spark.read.parquet(factsPath)
    Seq("buggy" -> buggyTopK(f), "fixed" -> fixedTwoStageTopK(f)).foreach { case (label, df) =>
      val t0 = System.nanoTime()
      val n = df.count()
      val ms = (System.nanoTime() - t0) / 1e6
      println(f"[WindowSkew] $label%-6s rows=$n%-12d ms=$ms%.0f")
    }
  }
}
