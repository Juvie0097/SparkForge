package com.sparkforge.join

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Three approaches to data-skew on join keys, each useful in a different
 * regime:
 *
 *  1. AQE skew-join (preferred since Spark 3.0):
 *     - Set `spark.sql.adaptive.skewJoin.enabled = true`
 *     - At runtime, AQE detects partitions whose size is > skewedPartitionFactor
 *       * median AND > skewedPartitionThresholdInBytes, then *splits* them
 *       across multiple tasks. Zero code change required.
 *     - Limitation: only applies to SMJ on shuffled tables; cannot help BHJ.
 *
 *  2. Manual salting (works pre-AQE and on RDD jobs):
 *     - Add a random salt to the hot key on the big side.
 *     - Explode the small side N times so every salted variant has a match.
 *     - Forces even distribution, but inflates the small side N×.
 *
 *  3. Hot/cold split (best when only a few keys are pathological):
 *     - Pull the known hot keys out, broadcast-join them separately,
 *     - Process the cold majority with the default plan, union results.
 *     - Best wall-clock when the hot key list is short (< few hundred).
 */
object SkewJoinHandler {

  /** Approach 1: just toggle AQE. Returned for parity with the other two. */
  def aqeSkewJoin(left: DataFrame, right: DataFrame, key: String)(implicit spark: SparkSession): DataFrame = {
    spark.conf.set("spark.sql.adaptive.enabled",                 "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled",        "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor",          "5")
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes","256MB")
    JoinStrategies.sortMergeJoin(left, right, key)
  }

  /**
   * Approach 2: salting.
   *
   * @param saltBuckets number of salt values (8–32 is usually enough). Larger
   *                    = more even but larger explosion of the small side.
   */
  def saltedJoin(left: DataFrame, right: DataFrame, key: String, saltBuckets: Int = 16): DataFrame = {
    val salts = (0 until saltBuckets).toArray

    val leftSalted = left
      .withColumn("_salt", (rand() * lit(saltBuckets)).cast("int"))
      .withColumn("_salted_key", concat_ws("#", col(key).cast("string"), col("_salt")))

    val rightExploded = right
      .withColumn("_salt", explode(lit(salts)))
      .withColumn("_salted_key", concat_ws("#", col(key).cast("string"), col("_salt")))

    leftSalted
      .join(rightExploded.drop(key), Seq("_salted_key"), "left")
      .drop("_salt", "_salted_key")
  }

  /**
   * Approach 3: hot/cold split.
   *
   * @param hotKeys precomputed list of pathological keys (e.g. via a pre-pass
   *                that ranks count(*) desc). Keep this list small.
   */
  def hotColdSplitJoin[T](
      left: DataFrame,
      right: DataFrame,
      key: String,
      hotKeys: Seq[T]
  ): DataFrame = {
    // Push the hot list as a literal array so the filter pushdown stays cheap
    // (Spark generates an `IN (...)` predicate that Parquet can use).
    val hotLit = array(hotKeys.map(k => lit(k)): _*)

    val leftHot  = left .filter( array_contains(hotLit, col(key)))
    val leftCold = left .filter(!array_contains(hotLit, col(key)))
    val rightHot = right.filter( array_contains(hotLit, col(key)))

    val hotJoined  = JoinStrategies.broadcastJoin(leftHot,  rightHot, key)
    val coldJoined = JoinStrategies.sortMergeJoin (leftCold, right,   key)
    hotJoined.unionByName(coldJoined)
  }

  /** Helper: rank the top-N hottest keys on a column (use to feed approach 3). */
  def topHotKeys(df: DataFrame, key: String, topN: Int = 50): Seq[Any] = {
    df.groupBy(key).count()
      .orderBy(col("count").desc)
      .limit(topN)
      .collect()
      .map(_.get(0))
      .toSeq
  }
}
