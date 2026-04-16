package com.sparkforge.advanced

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable

/**
 * Accumulators look like a free counter, but they have a subtle correctness
 * trap: if a task is re-run (speculation, FetchFailedException retry, stage
 * retry), its contribution is added AGAIN. The accumulator value is therefore
 * a *lower* bound, not the truth.
 *
 * Spark's docs are explicit:
 *   "For accumulator updates performed inside actions only, Spark guarantees
 *    that each task's update will only be applied once. In transformations,
 *    users should be aware that each task's update may be applied more than
 *    once if tasks or job stages are re-executed."
 *
 * Pattern below: dedupe by task attempt id so even if the task runs N times,
 * only one attempt contributes. Useful for "exactly once" stat counters when
 * the alternative (post-hoc count() over the output) is too expensive.
 */
object AccumulatorIdempotentDemo {

  def naive(spark: SparkSession, ds: Dataset[Long]): Long = {
    val acc = spark.sparkContext.longAccumulator("naive-counter")
    // Inside a transformation: subject to over-count under retries.
    ds.map { x =>
      acc.add(1L)
      x
    }.collect()
    acc.value
  }

  def idempotent(spark: SparkSession, ds: Dataset[Long]): Long = {
    val acc = spark.sparkContext.longAccumulator("idempotent-counter")

    // Track which (stage, partition, attempt) tuples we've counted.
    // Local to executor JVM — fine for de-duping retries within an executor.
    val seen = mutable.Set.empty[(Int, Int, Long)]

    ds.foreachPartition { it: Iterator[Long] =>
      val tc = org.apache.spark.TaskContext.get()
      val key = (tc.stageId(), tc.partitionId(), tc.taskAttemptId())
      if (seen.add(key)) {
        var c = 0L
        while (it.hasNext) { it.next(); c += 1L }
        acc.add(c)
      } else {
        // Already counted — drain iterator without contributing.
        while (it.hasNext) it.next()
      }
    }
    acc.value
  }

  /** A custom AccumulatorV2 that ignores subsequent merges from the same
   *  attempt. Survives across executor JVMs because dedupe key includes
   *  taskAttemptId, which is globally unique within a job. */
  class IdempotentLongAccumulator extends LongAccumulator {
    private val attempts = mutable.Set.empty[Long]
    override def merge(other: org.apache.spark.util.AccumulatorV2[java.lang.Long, java.lang.Long]): Unit = {
      val tc = org.apache.spark.TaskContext.get()
      val tid = if (tc != null) tc.taskAttemptId() else -1L
      if (attempts.add(tid)) super.merge(other)
    }
  }
}
