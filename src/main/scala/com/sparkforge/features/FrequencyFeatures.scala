package com.sparkforge.features

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

/**
 * Per-user transaction frequency over rolling 1/7/30-day windows.
 *
 * Implementation note: rather than computing each window via a separate
 * groupBy+filter (which would scan the table N times), we build *conditional
 * aggregations* using `when(...).otherwise(null)` and sum/count them in a
 * single shuffle. This is the same trick the Catalyst optimizer applies for
 * `FILTER (WHERE ...)` in standard SQL, and it cuts wall-clock by ~Nx.
 */
object FrequencyFeatures {

  /**
   * @param txns      successfully filtered transactions (status, ts available)
   * @param asOfTs    "now" cut-off — passed in instead of using current_timestamp()
   *                  so the job is deterministic and idempotent under retries.
   * @param windows   list of trailing window sizes in days (e.g. Seq(1,7,30))
   */
  def compute(txns: DataFrame, asOfTs: java.sql.Timestamp, windows: Seq[Int]): DataFrame = {
    val asOf = lit(asOfTs)

    // Build one Column per metric per window. Putting them all into a single
    // groupBy keeps shuffle to one stage.
    val aggs: Seq[Column] = windows.flatMap { w =>
      val inWin = (col("ts") >= expr(s"timestampadd(DAY, -$w, timestamp '${asOfTs.toString}')"))
        .and(col("ts") <= asOf)

      Seq(
        sum(when(inWin, lit(1L)).otherwise(lit(0L)))
          .alias(s"txn_cnt_${w}d"),
        sum(when(inWin and col("status") === "SUCCESS", lit(1L)).otherwise(lit(0L)))
          .alias(s"txn_succ_cnt_${w}d"),
        sum(when(inWin and col("status") === "FAIL",    lit(1L)).otherwise(lit(0L)))
          .alias(s"txn_fail_cnt_${w}d"),
        // approx_count_distinct uses HyperLogLog — bounded memory, ~2% error,
        // and *aggregable* across partitions unlike countDistinct.
        approx_count_distinct(when(inWin, col("merchant_id")), 0.02)
          .alias(s"distinct_merchants_${w}d"),
        approx_count_distinct(when(inWin, col("ip")), 0.02)
          .alias(s"distinct_ips_${w}d")
      )
    }

    txns.groupBy("user_id").agg(aggs.head, aggs.tail: _*)
  }
}
