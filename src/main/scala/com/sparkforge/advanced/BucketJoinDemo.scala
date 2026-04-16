package com.sparkforge.advanced

import com.sparkforge.config.AppConfig
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * Bucketed tables let Spark do a "shuffle-free join" when:
 *   - both sides bucket on the same column,
 *   - same number of buckets,
 *   - sort-by is also the join key (for SortMergeJoin to skip the sort).
 *
 * The catch is operational: bucketing only works when reading via
 * `spark.table(...)` (Hive metastore); reading via path loses the bucket spec
 * and Spark falls back to a normal shuffle. This is a frequent surprise in
 * production.
 */
object BucketJoinDemo {

  val numBuckets = 32

  /** Write the fact + user dim as bucketed Hive tables on `user_id`. */
  def materialise(spark: SparkSession): Unit = {
    val rawTxns  = spark.read.parquet(AppConfig.data.rawPath + "/fact_transactions")
    val rawUsers = spark.read.parquet(AppConfig.data.rawPath + "/dim_users")

    rawTxns.write
      .mode(SaveMode.Overwrite)
      .bucketBy(numBuckets, "user_id")
      .sortBy("user_id")
      .saveAsTable("bucketed_txns")

    rawUsers.write
      .mode(SaveMode.Overwrite)
      .bucketBy(numBuckets, "user_id")
      .sortBy("user_id")
      .saveAsTable("bucketed_users")
  }

  /** Read via `spark.table` — the bucket spec is preserved, so the join
   *  plans to SortMergeJoin without an Exchange node. */
  def joinShuffleFree(spark: SparkSession): Unit = {
    val t = spark.table("bucketed_txns")
    val u = spark.table("bucketed_users")
    val joined = t.join(u, "user_id")
    println("[BucketJoin] shuffle-free plan (no Exchange around the join):")
    joined.explain("formatted")
    println(s"[BucketJoin] count = ${joined.count()}")
  }

  /** Read via path — Spark forgets the bucket spec, falls back to shuffle. */
  def joinPathBased(spark: SparkSession): Unit = {
    // Resolve the warehouse location of the bucketed table from the catalog.
    val warehouse = spark.sessionState.catalog
      .getTableMetadata(org.apache.spark.sql.catalyst.TableIdentifier("bucketed_txns"))
      .location.toString

    val t = spark.read.parquet(warehouse)
    val u = spark.read.parquet(AppConfig.data.rawPath + "/dim_users")
    val joined = t.join(u, "user_id")
    println("[BucketJoin] path-based plan (Exchange present, bucket spec lost):")
    joined.explain("formatted")
  }
}
