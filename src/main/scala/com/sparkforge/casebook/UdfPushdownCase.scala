package com.sparkforge.casebook

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Case: UDF in a WHERE clause kills predicate pushdown.
 *
 * Symptom: explain shows `PushedFilters: []` and the read scans the whole
 * column instead of using Parquet row-group pruning.
 *
 * Cause: Catalyst is conservative about UDFs — it cannot reason about their
 * semantics, so it cannot push them through file readers. ANY UDF in a
 * filter blocks pushdown for the WHOLE filter, even if the rest is pure SQL.
 *
 * Fix: rewrite filters using built-in functions / `expr`. Built-ins are known
 * to Catalyst and translate cleanly to Parquet predicates.
 */
object UdfPushdownCase {

  /** Buggy: simple "amount > 500" wrapped in a Scala UDF. */
  def buggy(facts: DataFrame): DataFrame = {
    val isHigh = udf((amt: Double) => amt > 500.0)
    facts.filter(isHigh(col("amount")))
  }

  /** Fixed: use the built-in. Catalyst pushes `amount > 500` to Parquet. */
  def fixed(facts: DataFrame): DataFrame =
    facts.filter(col("amount") > 500.0)

  def demo(spark: SparkSession, factsPath: String): Unit = {
    val f = spark.read.parquet(factsPath)
    println("[UdfPushdown] BUGGY plan (no PushedFilters expected):")
    buggy(f).explain("formatted")
    println("[UdfPushdown] FIXED plan (PushedFilters: [GreaterThan(amount, 500.0)] expected):")
    fixed(f).explain("formatted")
  }
}
