package com.sparkforge.casebook

import com.sparkforge.compact.AdaptiveCoalescer
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * Case: dynamic partition write fan-out.
 *
 * Symptom:
 *   - Output directory has thousands of tiny files per partition.
 *   - Executor OOM right at the write stage with messages mentioning
 *     "ParquetOutputWriter" or "FileFormatWriter".
 *
 * Cause: every Spark task can hold one open writer per output partition. If
 * tasks have rows from many partitions, one task = many open writers = many
 * tiny files + memory blow-up.
 *
 * Fix: repartition by the partition columns before write, so each task
 * touches one (or few) output partitions. Costs one shuffle, saves disaster.
 *
 * Related knob: `spark.sql.sources.partitionOverwriteMode = dynamic` is
 * required if you want to overwrite *only* the touched partitions; the
 * default `static` mode wipes everything under the path.
 */
object DynamicPartitionWriteCase {

  def buggyWrite(df: DataFrame, path: String): Unit = {
    df.write.mode(SaveMode.Overwrite)
      .partitionBy("dt")
      .parquet(path)        // every task may open writers for every dt
  }

  def fixedWrite(df: DataFrame, path: String): Unit = {
    val rebalanced = AdaptiveCoalescer.repartitionForPartitionedWrite(df, Seq("dt"))
    rebalanced.write.mode(SaveMode.Overwrite)
      .partitionBy("dt")
      .option("maxRecordsPerFile", "5000000")
      .parquet(path)
  }

  /** When you want to overwrite only the partitions you touch — common in
   *  daily incremental jobs. */
  def fixedDynamicOverwrite(spark: SparkSession, df: DataFrame, path: String): Unit = {
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    fixedWrite(df, path)
  }
}
