package com.sparkforge.casebook

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Case: WholeStageCodegen 64 KB method limit.
 *
 * Symptom in the logs:
 *   org.codehaus.janino.InternalCompilerException: Code of method
 *   "processNext(...)V" of class "...GeneratedIteratorForCodegenStage..."
 *   grows beyond 64 KB
 *
 * Cause: Spark fuses operators of one stage into a single generated Java
 * method. JVM caps method bytecode at 64 KB. Pipelines with many `withColumn`
 * (hundreds of derived columns) blow past that.
 *
 * Fix options (in order of preference):
 *   1. Split the pipeline with an action / cache between large segments —
 *      breaks codegen fusion at the boundary.
 *   2. Lower `spark.sql.codegen.hugeMethodLimit` (default 65535) — Spark falls
 *      back to interpreter for that one stage instead of crashing.
 *   3. As a last resort, disable codegen: `spark.sql.codegen.wholeStage=false`
 *      (slow — maybe 2× regression — but keeps the job running).
 */
object CodegenLimitCase {

  /** Reproduce: chain N withColumn calls so the generated method grows huge. */
  def buggyManyWithColumn(facts: DataFrame, n: Int = 400): DataFrame = {
    var df = facts
    for (i <- 0 until n) {
      df = df.withColumn(s"f_$i",
        when(col("amount") > i, col("amount") + i).otherwise(col("amount") - i))
    }
    df
  }

  /** Fix 1: insert a cache barrier mid-pipeline. */
  def fixedSplit(facts: DataFrame, n: Int = 400): DataFrame = {
    val half = n / 2
    var df = facts
    for (i <- 0 until half) df = df.withColumn(s"f_$i", col("amount") + i)
    df = df.persist()
    df.count()       // materialise
    for (i <- half until n) df = df.withColumn(s"f_$i", col("amount") + i)
    df
  }

  /** Fix 2 + 3: knobs. */
  def applyKnobs(spark: SparkSession): Unit = {
    spark.conf.set("spark.sql.codegen.hugeMethodLimit", "8000") // cheaper fallback
    // spark.conf.set("spark.sql.codegen.wholeStage", "false")  // last resort
  }
}
