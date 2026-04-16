package com.sparkforge.tuning

import com.sparkforge.config.AppConfig
import org.apache.spark.sql.SparkSession

/**
 * Centralised AQE & shuffle-related Spark conf. Keeping every knob in one
 * file makes A/B comparisons trivial: flip a single `applyXxx(...)` call
 * in the entry-point, re-run, diff Spark UI.
 *
 * Reference (Spark 3.5):
 *   https://spark.apache.org/docs/3.5.1/sql-performance-tuning.html
 */
object AqeConfigurator {

  /** Production-quality default — what most jobs should use. */
  def applyRecommended(spark: SparkSession): Unit = {
    val t = AppConfig.tuning

    // --- AQE master switches ------------------------------------------
    set(spark, "spark.sql.adaptive.enabled",                  t.aqeEnabled.toString)
    set(spark, "spark.sql.adaptive.coalescePartitions.enabled", t.aqeCoalesceEnabled.toString)
    set(spark, "spark.sql.adaptive.skewJoin.enabled",         t.aqeSkewJoinEnabled.toString)
    set(spark, "spark.sql.adaptive.localShuffleReader.enabled", "true")

    // --- Partition sizing ---------------------------------------------
    set(spark, "spark.sql.adaptive.advisoryPartitionSizeInBytes", t.advisoryPartitionBytes)
    set(spark, "spark.sql.adaptive.coalescePartitions.minPartitionSize", "1MB")
    set(spark, "spark.sql.shuffle.partitions",                 t.shufflePartitions.toString)

    // --- Skew detection thresholds ------------------------------------
    set(spark, "spark.sql.adaptive.skewJoin.skewedPartitionFactor",          "5")
    set(spark, "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes","256MB")

    // --- Join strategy ------------------------------------------------
    set(spark, "spark.sql.autoBroadcastJoinThreshold", t.autoBroadcastThresholdBytes)
    // Convert SMJ -> BHJ at runtime when stats reveal one side is small.
    set(spark, "spark.sql.adaptive.autoBroadcastJoinThreshold", t.autoBroadcastThresholdBytes)

    // --- File reader sizing (small-file mitigation, read side) --------
    set(spark, "spark.sql.files.maxPartitionBytes", t.filesMaxPartitionBytes)
    set(spark, "spark.sql.files.openCostInBytes",   t.filesOpenCostBytes)
    set(spark, "spark.sql.files.maxRecordsPerFile", t.maxRecordsPerFile.toString)

    // --- Runtime bloom filter (Spark 3.3+) ----------------------------
    set(spark, "spark.sql.optimizer.runtime.bloomFilter.enabled", "true")

    // --- DPP ----------------------------------------------------------
    set(spark, "spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
  }

  /** Baseline = vanilla Spark, AQE off. Use as the "control" benchmark. */
  def applyBaseline(spark: SparkSession): Unit = {
    set(spark, "spark.sql.adaptive.enabled",                  "false")
    set(spark, "spark.sql.shuffle.partitions",                "200")
    set(spark, "spark.sql.autoBroadcastJoinThreshold",        "10MB")
  }

  /** Aggressive — for laptops or tiny datasets. Saves resources, not speed. */
  def applyLocalLaptop(spark: SparkSession): Unit = {
    applyRecommended(spark)
    set(spark, "spark.sql.shuffle.partitions",                "16")
    set(spark, "spark.sql.adaptive.advisoryPartitionSizeInBytes", "32MB")
  }

  private def set(spark: SparkSession, k: String, v: String): Unit = {
    spark.conf.set(k, v)
  }

  /** Dump every Spark/SQL conf that begins with `spark.sql.adaptive` — useful
   *  to verify nothing has been silently overridden by SparkConf or env vars. */
  def dumpAqeConfs(spark: SparkSession): Unit = {
    spark.conf.getAll
      .filter { case (k, _) => k.startsWith("spark.sql.adaptive") ||
                                k.startsWith("spark.sql.shuffle")  ||
                                k.startsWith("spark.sql.autoBroadcast") ||
                                k.startsWith("spark.sql.files") }
      .toSeq.sortBy(_._1)
      .foreach { case (k, v) => println(f"[conf] $k%-70s = $v") }
  }
}
