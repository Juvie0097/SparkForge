package com.sparkforge.config

import com.typesafe.config.{Config, ConfigFactory}

/**
 * Single typed view over `application.conf`. Kept intentionally flat so that
 * every module imports the same object without hidden coupling.
 *
 * Override at runtime with `-Dconfig.file=...` or `-Dsparkforge.<key>=<value>`.
 */
object AppConfig {
  private val root: Config = ConfigFactory.load().getConfig("sparkforge")

  val appName: String = root.getString("app-name")
  val master:  String = root.getString("master")

  object data {
    private val c = root.getConfig("data")
    val basePath:       String = c.getString("base-path")
    val rawPath:        String = c.getString("raw-path")
    val featurePath:    String = c.getString("feature-path")
    val compactPath:    String = c.getString("compact-path")
    val checkpointPath: String = c.getString("checkpoint-path")
  }

  object generator {
    private val c = root.getConfig("generator")
    val numUsers:           Int    = c.getInt("num-users")
    val numMerchants:       Int    = c.getInt("num-merchants")
    val numDevices:         Int    = c.getInt("num-devices")
    val numTransactions:    Long   = c.getLong("num-transactions")
    val daysBack:           Int    = c.getInt("days-back")
    val hotUserFraction:    Double = c.getDouble("hot-user-fraction")
    val hotUserCount:       Int    = c.getInt("hot-user-count")
    val rawWritePartitions: Int    = c.getInt("raw-write-partitions")
    val nullDeviceRate:     Double = c.getDouble("null-device-rate")
  }

  object features {
    private val c = root.getConfig("features")
    val windowsDays: Seq[Int] = {
      import scala.collection.JavaConverters._
      c.getIntList("windows-days").asScala.toSeq.map(_.intValue())
    }
    val cacheWideTable: Boolean = c.getBoolean("cache-wide-table")
  }

  object tuning {
    private val c = root.getConfig("tuning")
    val aqeEnabled:                Boolean = c.getBoolean("aqe-enabled")
    val aqeCoalesceEnabled:        Boolean = c.getBoolean("aqe-coalesce-enabled")
    val aqeSkewJoinEnabled:        Boolean = c.getBoolean("aqe-skew-join-enabled")
    val advisoryPartitionBytes:    String  = c.getString("advisory-partition-bytes")
    val autoBroadcastThresholdBytes: String = c.getString("autobroadcast-threshold-bytes")
    val shufflePartitions:         Int     = c.getInt("shuffle-partitions")
    val filesMaxPartitionBytes:    String  = c.getString("files-max-partition-bytes")
    val filesOpenCostBytes:        String  = c.getString("files-open-cost-bytes")
    val maxRecordsPerFile:         Long    = c.getLong("sql-files-max-records-per-file")
  }
}
