package com.sparkforge.casebook

import org.apache.spark.sql.SparkSession

/**
 * Case: long-tail straggler tasks.
 *
 * Symptom: Stage at 99% complete for a long time. Spark UI shows one task
 * still running while every other has finished. Common causes:
 *   - data skew that AQE didn't catch (e.g. groupBy)
 *   - a bad node (slow disk, network blip, GC storm)
 *   - external dependency (HTTP call inside mapPartitions)
 *
 * Mitigation: speculative execution. Spark launches duplicate copies of slow
 * tasks; whichever finishes first wins, the other is killed.
 *
 *   spark.speculation                   = true
 *   spark.speculation.multiplier        = 1.5    (task >1.5x median)
 *   spark.speculation.quantile          = 0.75   (only after 75% done)
 *   spark.speculation.minTaskRuntime    = 100ms
 *
 * IMPORTANT CAVEAT: speculation is unsafe for *non-idempotent* writes. If you
 * write to S3 / databases / message queues without transactional semantics,
 * the duplicate task can produce duplicate output. Either:
 *   - turn speculation off for write stages, or
 *   - use a writer that handles it (Iceberg, Delta, JDBC with INSERT IGNORE).
 */
object SpeculationCase {

  def enableSpeculation(spark: SparkSession): Unit = {
    spark.conf.set("spark.speculation",                "true")
    spark.conf.set("spark.speculation.multiplier",     "1.5")
    spark.conf.set("spark.speculation.quantile",       "0.75")
    spark.conf.set("spark.speculation.minTaskRuntime", "100ms")
    println("[Speculation] enabled — be sure your writers are idempotent.")
  }

  def disableForWriteStage(spark: SparkSession): Unit = {
    spark.conf.set("spark.speculation", "false")
    println("[Speculation] disabled for non-idempotent write stage.")
  }

  /**
   * Demonstrates the wrong fix: people often turn speculation on globally
   * because of one slow stage. That re-runs your `INSERT INTO mysql` statements,
   * which is the actual incident from production.
   */
  def antiPattern(spark: SparkSession): Unit = {
    enableSpeculation(spark)
    println("[Speculation] BAD: now every stage is speculative, including the JDBC writer")
  }
}
