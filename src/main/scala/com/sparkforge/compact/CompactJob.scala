package com.sparkforge.compact

import com.sparkforge.config.AppConfig
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * Stand-alone OPTIMIZE-style compaction job.
 *
 * Strategy:
 *   1. Analyze the path → decide if compaction is worthwhile (small ratio
 *      > threshold OR avg file size < lower bound). Skip if healthy.
 *   2. Read the path back as DataFrame, repartition to the target file count,
 *      write to a *staging* path (never overwrite in place — writes are not
 *      atomic on most FS, you'd lose data on failure).
 *   3. Atomic rename: delete original, rename staging → original.
 *
 * For production you'd use Iceberg `rewrite_data_files` / Delta `OPTIMIZE`
 * which handle the atomic swap via the metadata layer. This is the pure-Spark
 * equivalent for environments without a table format.
 */
object CompactJob {

  case class Result(beforeFiles: Long, afterFiles: Long, beforeMB: Double, afterMB: Double)

  def compact(
      spark: SparkSession,
      sourcePath: String,
      partitionCols: Seq[String] = Nil,
      smallRatioThreshold: Double = 0.30
  ): Option[Result] = {

    val before = FileSizeAnalyzer.analyze(spark, sourcePath, smallThresholdMB = 32)
    println(s"[compact] BEFORE  $sourcePath\n${before.pretty}")

    if (before.files == 0L) {
      println("[compact] empty source, skipping")
      return None
    }
    if (before.smallRatio < smallRatioThreshold && before.avgMB > 64) {
      println(f"[compact] healthy distribution (small=${before.smallRatio}%.2f), skipping")
      return None
    }

    val targetFiles = AdaptiveCoalescer.partitionsForBytes(before.totalBytes)
    println(s"[compact] rewriting to $targetFiles target file(s)")

    val staging = sourcePath.stripSuffix("/") + "._staging"

    // Read & rewrite — keep partitionBy alignment so directory layout is preserved.
    val df = spark.read.parquet(sourcePath)
    val rebalanced = if (partitionCols.nonEmpty) {
      AdaptiveCoalescer.repartitionForPartitionedWrite(df, partitionCols)
    } else {
      df.repartition(targetFiles)
    }

    val writer = rebalanced.write.mode(SaveMode.Overwrite)
      .format("parquet").option("compression", "zstd")
    val w2 = if (partitionCols.nonEmpty) writer.partitionBy(partitionCols: _*) else writer
    w2.save(staging)

    // Atomic-ish swap: delete source, then rename staging.
    // On HDFS this is two metadata ops; on S3 the rename is a copy+delete and
    // therefore not atomic — for S3 production use a table format.
    val fs = new Path(sourcePath).getFileSystem(spark.sparkContext.hadoopConfiguration)
    swapDirs(fs, sourcePath, staging)

    val after = FileSizeAnalyzer.analyze(spark, sourcePath, smallThresholdMB = 32)
    println(s"[compact] AFTER   $sourcePath\n${after.pretty}")

    Some(Result(before.files, after.files, before.totalMB, after.totalMB))
  }

  private def swapDirs(fs: FileSystem, source: String, staging: String): Unit = {
    val src = new Path(source)
    val stg = new Path(staging)
    if (fs.exists(src)) fs.delete(src, true)
    if (!fs.rename(stg, src))
      throw new RuntimeException(s"rename failed: $staging -> $source")
  }

  /** CLI helper: compact every dataset under `data/raw`. */
  def compactAll(spark: SparkSession): Unit = {
    val targets = Seq(
      ("/fact_transactions", Seq("dt")),
      ("/dim_users",         Seq.empty[String]),
      ("/dim_merchants",     Seq.empty[String]),
      ("/dim_devices",       Seq.empty[String])
    )
    targets.foreach { case (suffix, parts) =>
      compact(spark, AppConfig.data.rawPath + suffix, parts)
    }
  }
}
