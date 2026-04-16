package com.sparkforge.compact

import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SaveMode, SparkSession}

/**
 * Wraps the standard `DataFrameWriter` with file-size-aware defaults.
 *
 * Common pitfalls this prevents:
 *   1. Writing N tiny files because someone called `.repartition(2000)` upstream.
 *   2. The dynamic-partition fan-out: each task holds one writer per partition
 *      key, blowing up memory and producing N×P tiny files.
 *   3. Forgetting `maxRecordsPerFile`, leaving a single partition with
 *      a 5-GB file that no downstream reader can split.
 */
object WriteOptimizer {

  /**
   * @param df               the dataset to write
   * @param path             target path
   * @param partitionCols    partition columns (e.g. Seq("dt"))
   * @param targetFileMB     target file size in MB (Parquet writer hint)
   * @param maxRecordsPerFile cap rows per file → bounds individual file size
   * @param compression      "zstd" | "snappy" | "gzip"
   */
  def writePartitioned(
      df: DataFrame,
      path: String,
      partitionCols: Seq[String],
      targetFileMB: Int = 192,
      maxRecordsPerFile: Long = 5000000L,
      compression: String = "zstd"
  ): Unit = {
    // 1. Repartition by partition cols → each task writes 1 partition only.
    //    Without this, every task fans out to every partition.
    val rebalanced = if (partitionCols.nonEmpty) {
      AdaptiveCoalescer.repartitionForPartitionedWrite(df, partitionCols)
    } else df

    val writer: DataFrameWriter[Row] = rebalanced.write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .option("compression", compression)
      // Row-group size hint — too small = bad scan throughput, too big = bad
      // predicate-pushdown granularity. 128 MB is a battle-tested sweet spot.
      .option("parquet.block.size", (targetFileMB * 1024 * 1024).toString)
      .option("maxRecordsPerFile", maxRecordsPerFile.toString)

    val withPart = if (partitionCols.nonEmpty) writer.partitionBy(partitionCols: _*) else writer
    withPart.save(path)
  }

  /**
   * Sized-write helper for non-partitioned outputs. Computes the partition
   * count from a sample-based byte estimate so output files land near target.
   */
  def writeWithTargetSize(
      df: DataFrame,
      path: String,
      targetFileMB: Int = 192,
      compression: String = "zstd"
  )(implicit spark: SparkSession): Unit = {
    // Cheap estimate: ask Catalyst's stats engine. If stats are missing
    // (no ANALYZE TABLE / non-Hive source), fall back to row count * estimated
    // bytes-per-row. We compute count() once anyway as part of materialisation.
    val statsBytes = df.queryExecution.optimizedPlan.stats.sizeInBytes.toLong
    val estimatedBytes =
      if (statsBytes > 0L && statsBytes < Long.MaxValue / 2) statsBytes
      else df.count() * 256L      // 256 B / row is a conservative default

    val parts = AdaptiveCoalescer.partitionsForBytes(estimatedBytes)

    df.repartition(parts).write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .option("compression", compression)
      .option("parquet.block.size", (targetFileMB * 1024 * 1024).toString)
      .save(path)
    // `spark` is intentionally an implicit handle so callers can inject the
    // session without re-importing — kept here even though the helper only
    // needs Catalyst stats from the DF's own plan.
    val _ = spark
  }
}
