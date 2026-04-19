package com.sparkforge.advanced

import com.sparkforge.config.AppConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * Dynamic Partition Pruning (DPP).
 *
 * Idea: when a partitioned fact table joins to a dim table that has a filter,
 * Spark can run the dim filter first, collect the matching partition keys,
 * and broadcast them back as a runtime predicate that *prunes partitions*
 * on the fact table. Avoids reading data you'd later throw away.
 *
 * Required preconditions:
 *   - `spark.sql.optimizer.dynamicPartitionPruning.enabled = true` (default)
 *   - Fact side must be partitioned by the join key (or a column derivable
 *     from it).
 *   - Dim side must be small enough to broadcast OR DPP "reuse exchange" mode
 *     applies (Spark 3.x).
 *
 * Verify by looking for `PartitionFilters: [..., dynamicpruning(...)]` in
 * the explain output.
 */
object DppDemo {

  // Dedicated staging path so setUp never touches the shared fact_transactions.
  private def dppFactPath = AppConfig.data.rawPath + "/fact_transactions_dpp"

  /** Build a join where `dt` is the partition column on the fact and is
   *  filterable on the dim side via `recent_dt`. */
  def setUp(spark: SparkSession): Unit = {
    import spark.implicits._
    // Read original, write to a separate DPP-specific path so the shared
    // fact_transactions is never overwritten mid-read.
    val txns = spark.read.parquet(AppConfig.data.rawPath + "/fact_transactions")

    // dim_dt: tiny calendar dim, holds "is_recent" flag
    val cal = (0 until 35).map(i =>
      (java.sql.Date.valueOf(java.time.LocalDate.now().minusDays(i)), i < 7)
    )
    cal.toDF("dt", "is_recent")
      .write.mode(SaveMode.Overwrite).parquet(AppConfig.data.rawPath + "/dim_dt")

    txns.write.mode(SaveMode.Overwrite)
      .partitionBy("dt").parquet(dppFactPath)
  }

  def runWithDpp(spark: SparkSession): Unit = {
    spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")

    val fact = spark.read.parquet(dppFactPath)
    val cal  = spark.read.parquet(AppConfig.data.rawPath + "/dim_dt")

    val q = fact.join(broadcast(cal.filter(col("is_recent"))), Seq("dt"))
    println("[DPP ON] expect PartitionFilters with dynamicpruning():")
    q.explain("formatted")
    println(s"[DPP ON] rows = ${q.count()}")
  }

  def runWithoutDpp(spark: SparkSession): Unit = {
    spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "false")
    val fact = spark.read.parquet(dppFactPath)
    val cal  = spark.read.parquet(AppConfig.data.rawPath + "/dim_dt")
    val q = fact.join(broadcast(cal.filter(col("is_recent"))), Seq("dt"))
    println("[DPP OFF] expect PartitionFilters: [] — full scan:")
    q.explain("formatted")
  }
}
