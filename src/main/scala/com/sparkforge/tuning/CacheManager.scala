package com.sparkforge.tuning

import com.sparkforge.config.AppConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

/**
 * Opinionated DataFrame cache manager.
 *
 * Why not just call `df.cache()` everywhere?
 *  - Default storage level is MEMORY_AND_DISK (deserialised), which on wide
 *    rows with many columns can be 3–5× the on-disk size and triggers GC
 *    pressure. SER variants are usually a better default for "intermediate
 *    pipeline" tables.
 *  - Naked cache() leaks: tables persist across stages and quietly evict the
 *    things you actually wanted in memory. Tracking + explicit unpersist
 *    keeps the storage tab in Spark UI honest.
 *  - Long lineage chains (10+ joins) eventually OOM the driver during plan
 *    serialization. A periodic checkpoint truncates lineage; this manager
 *    chooses based on intended reuse count.
 */
object CacheManager {

  private val tracked: mutable.Map[String, DataFrame] = mutable.Map.empty

  /** Wide intermediate tables that will be reused 3+ times. */
  def persistWide(df: DataFrame, name: String): DataFrame = {
    val cached = df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    cached.count()                    // force materialisation NOW
    tracked.update(name, cached)
    println(s"[cache] persisted $name with MEMORY_AND_DISK_SER")
    cached
  }

  /** Small frequently-touched lookups. Kept hot in deserialised form. */
  def persistSmall(df: DataFrame, name: String): DataFrame = {
    val cached = df.persist(StorageLevel.MEMORY_ONLY)
    cached.count()
    tracked.update(name, cached)
    cached
  }

  /** Used when the table is too big for memory but cheaper than recompute. */
  def persistDiskOnly(df: DataFrame, name: String): DataFrame = {
    val cached = df.persist(StorageLevel.DISK_ONLY)
    cached.count()
    tracked.update(name, cached)
    cached
  }

  /**
   * Cuts off DAG history. Call this when a wide table feeds many downstream
   * stages and you want plan-serialization size to stay bounded. Requires
   * `spark.sparkContext.setCheckpointDir` to be set first.
   */
  def truncateLineage(spark: SparkSession, df: DataFrame, name: String): DataFrame = {
    if (spark.sparkContext.getCheckpointDir.isEmpty) {
      spark.sparkContext.setCheckpointDir(AppConfig.data.checkpointPath)
    }
    val cp = df.checkpoint(eager = true)   // eager so plan is truncated immediately
    tracked.update(s"$name.cp", cp)
    cp
  }

  def unpersist(name: String): Unit = tracked.remove(name).foreach { df =>
    df.unpersist(blocking = false)
    println(s"[cache] unpersisted $name")
  }

  def unpersistAll(): Unit = {
    tracked.foreach { case (n, df) =>
      df.unpersist(blocking = false)
      println(s"[cache] unpersisted $n")
    }
    tracked.clear()
  }

  /** Print the storage tab summary so we know what's actually in memory. */
  def report(spark: SparkSession): Unit = {
    val mgr = spark.sparkContext.getRDDStorageInfo
    if (mgr.isEmpty) println("[cache] nothing materialised")
    mgr.foreach { info =>
      println(f"[cache] rdd=${info.id}%4d name=${info.name}%-40s " +
              f"memSize=${info.memSize / (1024L * 1024L)}%6dMB " +
              f"diskSize=${info.diskSize / (1024L * 1024L)}%6dMB " +
              f"partitions=${info.numPartitions}")
    }
  }
}
