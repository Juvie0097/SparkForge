package com.sparkforge.compact

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.SparkSession

/**
 * Walks a path on the configured FileSystem (HDFS/S3/local) and reports the
 * file-size distribution. Used as the diagnostic step before compaction:
 *
 *   - "small" file = file < 32 MB (configurable)
 *   - target file size 128–256 MB aligns with HDFS block & Parquet row-group
 *
 * Avoids `hdfs dfs -du -h` because that doesn't break out by file count.
 */
object FileSizeAnalyzer {

  case class Stats(
    files:         Long,
    totalBytes:    Long,
    smallFiles:    Long,
    smallBytes:    Long,
    p50Bytes:      Long,
    p95Bytes:      Long,
    minBytes:      Long,
    maxBytes:      Long
  ) {
    def smallRatio: Double = if (files == 0) 0.0 else smallFiles.toDouble / files
    def avgMB:      Double = if (files == 0) 0.0 else totalBytes.toDouble / files / (1024 * 1024)
    def totalMB:    Double = totalBytes.toDouble / (1024 * 1024)

    def pretty: String = {
      val mbP50 = p50Bytes / 1024.0 / 1024.0
      val mbP95 = p95Bytes / 1024.0 / 1024.0
      val mbMin = minBytes / 1024.0 / 1024.0
      val mbMax = maxBytes / 1024.0 / 1024.0
      val pct   = smallRatio * 100.0
      f"""files=$files%-10d totalMB=$totalMB%-10.1f avgMB=$avgMB%-7.1f
         |  p50=$mbP50%-6.1fMB  p95=$mbP95%-6.1fMB
         |  min=$mbMin%-6.1fMB  max=$mbMax%-6.1fMB
         |  small(<32MB)=$smallFiles ($pct%.1f%%)
         |""".stripMargin
    }
  }

  /** @param smallThresholdMB anything strictly smaller is counted as "small". */
  def analyze(spark: SparkSession, path: String, smallThresholdMB: Int = 32): Stats = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val p          = new Path(path)
    val fs         = p.getFileSystem(hadoopConf)

    val files = listFilesRec(fs, p).filter(_.getLen > 0L)
    if (files.isEmpty) return Stats(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L)

    val sizes      = files.map(_.getLen).sorted.toArray
    val total      = sizes.sum
    val threshold  = smallThresholdMB.toLong * 1024L * 1024L
    val small      = sizes.count(_ < threshold)
    val smallBytes = sizes.filter(_ < threshold).sum
    val p50        = sizes((sizes.length * 0.50).toInt min (sizes.length - 1))
    val p95        = sizes((sizes.length * 0.95).toInt min (sizes.length - 1))

    Stats(sizes.length, total, small, smallBytes, p50, p95, sizes.head, sizes.last)
  }

  /** Recursive listing — necessary for partitioned tables. */
  private def listFilesRec(fs: org.apache.hadoop.fs.FileSystem, p: Path): Seq[FileStatus] = {
    if (!fs.exists(p)) return Seq.empty
    val it = fs.listFiles(p, true)   // recursive, hides hidden _SUCCESS etc.
    val buf = scala.collection.mutable.ArrayBuffer.empty[FileStatus]
    while (it.hasNext) {
      val s = it.next()
      val name = s.getPath.getName
      if (s.isFile && !name.startsWith(".") && !name.startsWith("_")) buf += s
    }
    buf.toSeq
  }
}
