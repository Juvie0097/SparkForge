package com.sparkforge.compact

import org.apache.spark.sql.{Column, DataFrame}

/**
 * Picks an output partition count so that resulting files sit close to the
 * target size. Two flavours:
 *
 *   - byRowCount   : when an upstream count() has already been paid for.
 *   - byInputBytes : when only the input size estimate is known (cheap but
 *                    less accurate after filters).
 *
 * Why not just `coalesce(N)` or `repartition(N)` blindly?
 *   - coalesce(N) skips the shuffle but inherits parent partition skew → some
 *     output files are huge, others tiny. Use only when you're sure parent
 *     partitions are roughly even.
 *   - repartition(N) reshuffles → even files, costs one shuffle.
 *   - For partitioned writes, repartition by the partition columns to avoid
 *     fan-out (one task writing to every partition).
 */
object AdaptiveCoalescer {

  // Aim for files sized between 128MB and 256MB
  val TargetMinBytes: Long = 128L * 1024L * 1024L
  val TargetMaxBytes: Long = 256L * 1024L * 1024L

  /** Compute a partition count given total output bytes. */
  def partitionsForBytes(totalBytes: Long): Int = {
    val targetAvg = (TargetMinBytes + TargetMaxBytes) / 2
    math.max(1, math.ceil(totalBytes.toDouble / targetAvg).toInt)
  }

  /** Heuristic from row count + estimated bytes-per-row. */
  def partitionsForRows(rows: Long, bytesPerRow: Int): Int =
    partitionsForBytes(rows * bytesPerRow.toLong)

  /**
   * Repartition the DF by the given partition columns *plus* a salt, capped
   * at `maxFilesPerPartition` files per partition. Avoids the dynamic-write
   * fan-out problem (a task opening one writer per output partition).
   */
  def repartitionForPartitionedWrite(
      df: DataFrame,
      partitionCols: Seq[String],
      filesPerPartition: Int = 1
  ): DataFrame = {
    require(partitionCols.nonEmpty, "must supply partition columns")
    val keys: Seq[Column] = partitionCols.map(df.col)
    if (filesPerPartition <= 1) {
      df.repartition(keys: _*)
    } else {
      // Add a salt column so each partition gets `filesPerPartition` shards.
      val salted = df.withColumn("__file_salt",
        (org.apache.spark.sql.functions.rand() * filesPerPartition).cast("int"))
      salted
        .repartition((keys :+ salted.col("__file_salt")): _*)
        .drop("__file_salt")
    }
  }
}
