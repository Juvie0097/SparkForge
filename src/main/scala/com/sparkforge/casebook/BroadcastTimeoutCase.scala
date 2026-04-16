package com.sparkforge.casebook

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Case: BroadcastTimeoutException.
 *
 * Symptom:
 *   org.apache.spark.SparkException: Could not execute broadcast in N secs.
 *   You can increase spark.sql.broadcastTimeout or disable broadcast join.
 *
 * Causes (in order of likelihood):
 *   1. The "small" side is actually big — stale stats, or a filter that
 *      Catalyst couldn't push down before the broadcast was planned.
 *   2. Driver is starved (low memory, GC pressure) and can't collect+pack
 *      the broadcast in time.
 *   3. Network from driver to executors is the bottleneck.
 *
 * Wrong fix: blindly bumping spark.sql.broadcastTimeout. Treats the symptom,
 * mask a real OOM-in-waiting on the driver.
 *
 * Right fixes:
 *   - Tighten autoBroadcastJoinThreshold so Catalyst falls back to SMJ.
 *   - For the rare case where you really want broadcast, materialise the small
 *     side first (collect → broadcast variable) so the broadcast cost is
 *     deterministic.
 */
object BroadcastTimeoutCase {

  def buggyForceBroadcast(facts: DataFrame, allegedlySmall: DataFrame): DataFrame = {
    // Forcing broadcast on a side that is actually big → timeout.
    facts.join(broadcast(allegedlySmall), Seq("user_id"), "left")
  }

  def fixedFallbackToSmj(facts: DataFrame, dim: DataFrame)(implicit spark: SparkSession): DataFrame = {
    // Lower the threshold so Catalyst won't auto-broadcast; merge hint enforces SMJ.
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")
    facts.hint("merge").join(dim, Seq("user_id"), "left")
  }

  /** When a deliberate broadcast is required: collect once and pin via a
   *  broadcast variable. Catches "too big" early, on the driver, instead of
   *  timing out across the cluster. */
  def fixedExplicitBroadcastVar(spark: SparkSession, facts: DataFrame, smallDim: DataFrame): DataFrame = {
    import spark.implicits._
    // Project to just the columns we need before collect — never broadcast
    // a wide row. If this line OOMs the driver, that is the *point*: we want
    // to fail fast and obviously rather than time-out mysteriously later.
    val rows = smallDim.select("user_id", "user_name").as[(Long, String)].collect()
    val bv   = spark.sparkContext.broadcast(rows.toMap)

    val lookup = udf((uid: Long) => bv.value.getOrElse(uid, null))
    facts.withColumn("user_name", lookup(col("user_id")))
  }
}
