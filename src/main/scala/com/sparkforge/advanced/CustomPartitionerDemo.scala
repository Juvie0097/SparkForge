package com.sparkforge.advanced

import org.apache.spark.{HashPartitioner, Partitioner, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * RDD-level custom partitioner.
 *
 * The DataFrame API hides the partitioner; for SQL-typed jobs you should
 * almost always use AQE + skewJoin instead of writing one of these. But the
 * RDD layer is still where some legacy ETL lives, and a custom partitioner
 * is the right escape-hatch when you need to:
 *   - keep certain keys on certain executors (locality with external state),
 *   - implement range partitioning that the hash partitioner can't,
 *   - isolate hot keys to a small set of partitions.
 *
 * This demo: keep all "hot" user_ids in a dedicated partition, distribute
 * everyone else uniformly across the rest. Hot reducers can then be run with
 * extra resources / speculation specifically.
 */
class HotKeyAwarePartitioner(
    hotKeys: Set[Long],
    coldPartitions: Int
) extends Partitioner {

  override def numPartitions: Int = coldPartitions + 1   // last one is "hot"

  override def getPartition(key: Any): Int = key match {
    case k: Long if hotKeys.contains(k) => coldPartitions  // dedicated bucket
    case k                              =>
      // Stable hash that doesn't collide with the hot bucket.
      math.abs(k.hashCode()) % coldPartitions
  }
}

object CustomPartitionerDemo {

  def run(sc: SparkContext, rdd: RDD[(Long, Double)], hotKeys: Set[Long]): RDD[(Long, Double)] = {
    val cold = math.max(8, sc.defaultParallelism)
    val parted = rdd.partitionBy(new HotKeyAwarePartitioner(hotKeys, cold))

    println(s"[CustomPartitioner] partitions=${parted.getNumPartitions} (cold=$cold + hot=1)")
    // Show how many records ended up in each partition — last one is the
    // hot bucket. Useful to confirm separation.
    parted.mapPartitionsWithIndex { case (idx, it) =>
      Iterator((idx, it.size.toLong))
    }.collect().sortBy(_._1).foreach { case (i, n) =>
      println(f"[CustomPartitioner] partition=$i%4d records=$n%-12d")
    }
    parted
  }

  /** Compare against vanilla HashPartitioner. */
  def compareWithDefault(sc: SparkContext, rdd: RDD[(Long, Double)]): Unit = {
    val n = math.max(8, sc.defaultParallelism)
    val parted = rdd.partitionBy(new HashPartitioner(n))
    parted.mapPartitionsWithIndex { case (idx, it) =>
      Iterator((idx, it.size.toLong))
    }.collect().sortBy(-_._2).take(5).foreach { case (i, c) =>
      println(f"[HashPart] top  partition=$i%4d records=$c%-12d")
    }
  }
}
