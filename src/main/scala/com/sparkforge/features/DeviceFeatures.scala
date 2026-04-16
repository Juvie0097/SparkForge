package com.sparkforge.features

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

/**
 * Device & geo fingerprint features.
 *
 * Two flavors are provided:
 *  - aggregate counts (HLL-based, cheap)
 *  - first/last device per window (uses windowing — expensive, gated to a
 *    single 30-day window to bound cost).
 *
 * NULL device_id rows are dropped *before* aggregation. We deliberately do
 * not do this in the join layer so that downstream casebook demos can show
 * the NULL-key skew problem.
 */
object DeviceFeatures {

  def compute(txns: DataFrame, asOfTs: java.sql.Timestamp, windows: Seq[Int]): DataFrame = {
    val cleaned = txns.filter(col("device_id").isNotNull)

    val aggs: Seq[Column] = windows.flatMap { w =>
      val inWin = col("ts") >= expr(
        s"timestampadd(DAY, -$w, timestamp '${asOfTs.toString}')")

      Seq(
        approx_count_distinct(when(inWin, col("device_id")), 0.02)
          .alias(s"distinct_devices_${w}d"),
        approx_count_distinct(when(inWin, col("geo_hash")),  0.02)
          .alias(s"distinct_geos_${w}d"),
        approx_count_distinct(when(inWin, col("ip")),        0.02)
          .alias(s"distinct_ips_dev_${w}d"),
        // Channel diversity: cheap because cardinality is tiny (3)
        countDistinct(when(inWin, col("channel")))
          .alias(s"distinct_channels_${w}d")
      )
    }
    val agged = cleaned.groupBy("user_id").agg(aggs.head, aggs.tail: _*)

    // First / last device over the 30d window. Window functions sort within
    // partition, so this is the priciest step here — keep it scoped tight.
    val w30Start = expr(s"timestampadd(DAY, -30, timestamp '${asOfTs.toString}')")
    val win = Window.partitionBy("user_id").orderBy(col("ts").asc)
    val firstLast = cleaned
      .filter(col("ts") >= w30Start)
      .withColumn("first_dev", first(col("device_id")).over(win))
      .withColumn("last_dev",  last(col("device_id"))
        .over(win.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
      .groupBy("user_id")
      .agg(
        first("first_dev").alias("first_device_30d"),
        first("last_dev") .alias("last_device_30d")
      )

    agged.join(firstLast, Seq("user_id"), "left")
  }
}
