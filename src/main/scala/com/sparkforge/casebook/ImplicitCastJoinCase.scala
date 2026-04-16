package com.sparkforge.casebook

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType}

/**
 * Case: implicit type cast on join key.
 *
 * Symptom:
 *   - explain shows `cast(user_id#xxx as string)` on one side
 *   - PartitionFilters / PushedFilters lose `user_id` (because the cast hides
 *     the original column from Parquet)
 *   - Read time goes up 3–5×; whole-stage codegen sometimes also breaks
 *
 * Cause: the two tables disagree on the type of `user_id`. Catalyst inserts
 * an implicit cast on the smaller-rank side, which prevents predicate
 * pushdown to Parquet and forces a full-column read.
 *
 * Fix: align the types upstream — either cast at source/ETL time, or cast
 * the smaller side eagerly *and* persist so the cast cost is paid once.
 */
object ImplicitCastJoinCase {

  def buggy(facts: DataFrame, dim: DataFrame): DataFrame = {
    // Pretend dim was loaded with user_id as STRING (a common ETL mistake).
    val dimStr = dim.withColumnRenamed("user_id", "user_id_str")
                    .withColumn("user_id_str", col("user_id_str").cast(StringType))
    // Explicit `===` keeps the cast visible in the plan; using
    // `Seq("user_id")` would trip Spark's resolver and either error out
    // immediately or quietly cast — either way, less educational.
    facts.join(dimStr, facts("user_id") === dimStr("user_id_str"), "left")
  }

  def fixed(facts: DataFrame, dim: DataFrame): DataFrame = {
    // Align types eagerly on the small side; large side stays as LongType so
    // the join hash matches the partitioner.
    val dimAligned = dim.withColumn("user_id", col("user_id").cast(LongType))
    facts.join(dimAligned, Seq("user_id"), "left")
  }

  def demo(spark: SparkSession, factsPath: String, dimPath: String): Unit = {
    val f = spark.read.parquet(factsPath)
    val d = spark.read.parquet(dimPath)
    val b = buggy(f, d)
    val g = fixed(f, d)
    println("[ImplicitCast] BUGGY plan:")
    b.explain("formatted")
    println("[ImplicitCast] FIXED plan:")
    g.explain("formatted")
    println(s"[ImplicitCast] buggy rows=${b.count()}  fixed rows=${g.count()}")
  }
}
