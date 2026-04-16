package com.sparkforge.features

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

/**
 * Per-user amount distribution over rolling windows.
 *
 * Uses `percentile_approx` (t-digest variant in Spark 3.x) instead of exact
 * `percentile`, because:
 *  - exact percentile needs a full sort within each group → O(N log N) per key
 *    and frequently OOMs on hot keys
 *  - approx is mergeable, ~1% error at default accuracy=10000, and orders of
 *    magnitude cheaper.
 */
object AmountFeatures {

  def compute(txns: DataFrame, asOfTs: java.sql.Timestamp, windows: Seq[Int]): DataFrame = {
    val aggs: Seq[Column] = windows.flatMap { w =>
      val inWin = col("ts") >= expr(
        s"timestampadd(DAY, -$w, timestamp '${asOfTs.toString}')")

      // Use NULL-out trick: when not in window, value becomes NULL and is
      // ignored by aggregations — equivalent to a filter but lets us share
      // a single groupBy across every window.
      val maskedAmt = when(inWin, col("amount"))

      Seq(
        sum(maskedAmt)         .alias(s"amt_sum_${w}d"),
        avg(maskedAmt)         .alias(s"amt_avg_${w}d"),
        min(maskedAmt)         .alias(s"amt_min_${w}d"),
        max(maskedAmt)         .alias(s"amt_max_${w}d"),
        stddev_samp(maskedAmt) .alias(s"amt_std_${w}d"),
        percentile_approx(maskedAmt, lit(0.50), lit(10000)).alias(s"amt_p50_${w}d"),
        percentile_approx(maskedAmt, lit(0.95), lit(10000)).alias(s"amt_p95_${w}d"),
        percentile_approx(maskedAmt, lit(0.99), lit(10000)).alias(s"amt_p99_${w}d")
      )
    }

    txns.groupBy("user_id").agg(aggs.head, aggs.tail: _*)
  }
}
