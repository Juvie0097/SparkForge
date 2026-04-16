package com.sparkforge.features

import com.sparkforge.config.AppConfig
import com.sparkforge.join.JoinStrategies
import com.sparkforge.tuning.CacheManager
import com.sparkforge.util.ExplainHelper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * End-to-end feature pipeline. Orchestration only — heavy logic lives in the
 * per-aspect modules so each can be benchmarked independently.
 *
 * The order of operations is deliberate and matters for performance:
 *   1. Read raw fact + dims (column-pruned & predicate-pushed).
 *   2. Enrich the *fact* with the small dims via broadcast joins (cheap,
 *      no shuffle) BEFORE doing the heavy groupBy. Doing it after would
 *      require a second join over the wide aggregate table.
 *   3. Cache the enriched fact so every feature aspect reads from memory.
 *   4. Compute frequency/amount/device aspects in parallel, each producing
 *      a per-user table.
 *   5. Outer-join the aspects on user_id (small, narrow → broadcast / SMJ).
 *   6. Optionally checkpoint the wide table to truncate lineage before write.
 */
object FeaturePipeline {

  def run(spark: SparkSession): DataFrame = {
    val asOf = java.sql.Timestamp.valueOf(java.time.LocalDateTime.now())

    // -- 1. read ----------------------------------------------------------
    val facts = spark.read.parquet(AppConfig.data.rawPath + "/fact_transactions")
      // Column-pruning happens automatically when we project, but being
      // explicit makes intent clear and survives later refactors.
      .select("user_id", "merchant_id", "device_id", "ip", "geo_hash",
              "amount", "channel", "status", "ts", "dt")

    val merchants = spark.read.parquet(AppConfig.data.rawPath + "/dim_merchants")
    val mcc       = spark.read.parquet(AppConfig.data.rawPath + "/dim_mcc")

    // -- 2. enrich (broadcast small dims) ---------------------------------
    // dim_merchants is ~5k rows — well under default broadcast threshold,
    // but we hint explicitly so the plan is stable across stat refreshes.
    val enriched = JoinStrategies.broadcastJoin(facts, merchants, "merchant_id")
      .transform(df => JoinStrategies.broadcastJoin(df, mcc, "mcc"))

    ExplainHelper.dumpPlan(enriched, "enriched_fact")

    // -- 3. cache ---------------------------------------------------------
    val maybeCached = if (AppConfig.features.cacheWideTable) {
      CacheManager.persistWide(enriched, "enriched_fact")
    } else enriched

    // -- 4. per-aspect features ------------------------------------------
    val freq   = FrequencyFeatures.compute(maybeCached, asOf, AppConfig.features.windowsDays)
    val amt    = AmountFeatures   .compute(maybeCached, asOf, AppConfig.features.windowsDays)
    val device = DeviceFeatures   .compute(maybeCached, asOf, AppConfig.features.windowsDays)

    // -- 5. assemble wide table ------------------------------------------
    val wide = freq
      .join(amt,    Seq("user_id"), "outer")
      .join(device, Seq("user_id"), "outer")
      .withColumn("compute_dt", to_date(lit(asOf)))

    ExplainHelper.dumpPlan(wide, "user_wide_features")
    wide
  }

  /** Run + persist. Splits read & write paths so it's easy to test `run` alone. */
  def runAndWrite(spark: SparkSession): Unit = {
    val wide = run(spark)
    wide.write
      .mode(SaveMode.Overwrite)
      .partitionBy("compute_dt")
      .option("compression", "zstd")
      .parquet(AppConfig.data.featurePath + "/user_wide")
  }
}
