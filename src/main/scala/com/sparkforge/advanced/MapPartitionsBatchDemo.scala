package com.sparkforge.advanced

import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * `mapPartitions` lets you batch per-row work into per-partition work. Useful
 * when each row triggers an expensive setup (HTTP client, model load, JDBC
 * connection, etc).
 *
 * Two common gotchas this demo highlights:
 *   1. Initialise the resource INSIDE the iterator factory — initialising
 *      outside leaks across executors and can serialize non-serializable refs.
 *   2. Always exhaust the iterator before returning, so resources can be
 *      released in the same partition.
 *
 * For real ML inference / external service calls, consider Spark 3.5's
 * `mapInPandas` (PySpark) or Arrow-based UDFs for vectorised batching.
 */
object MapPartitionsBatchDemo {

  /** Bad: per-row HTTP-style call. */
  def naive(spark: SparkSession, ds: Dataset[Long]): Dataset[(Long, Long)] = {
    import spark.implicits._
    ds.map(x => (x, expensiveScore(x)))
  }

  /** Good: open one client per partition, batch 256 rows at a time. */
  def batched(spark: SparkSession, ds: Dataset[Long]): Dataset[(Long, Long)] = {
    import spark.implicits._
    ds.mapPartitions { it =>
      val client = openClient()                 // one per partition
      try {
        it.grouped(256).flatMap { batch =>
          val scores = client.batchScore(batch.toArray)
          batch.zip(scores).map { case (k, s) => (k, s) }
        }
      } finally {
        client.close()
      }
    }
  }

  // ------ stand-ins for real external dependencies ----------------------
  private def expensiveScore(x: Long): Long = { Thread.sleep(0); x * 7 + 3 }

  private trait BatchClient {
    def batchScore(xs: Array[Long]): Array[Long]
    def close(): Unit
  }
  private def openClient(): BatchClient = new BatchClient {
    override def batchScore(xs: Array[Long]): Array[Long] = xs.map(_ * 7 + 3)
    override def close(): Unit = {}
  }
}
