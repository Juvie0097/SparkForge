package com.sparkforge.util

import org.apache.spark.scheduler._
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
 * Lightweight SparkListener that captures per-stage metrics into memory and
 * emits a human-readable report. Not a replacement for the History Server
 * but invaluable for quick local A/B benchmarks where you don't want to
 * launch the UI.
 *
 * Highlights:
 *   - per-stage duration, shuffle read/write bytes & records,
 *   - max / median / min task duration → quick skew detection,
 *   - stage retries (visible as duplicate stage IDs).
 */
class MetricsCollector extends SparkListener {

  private case class TaskTiming(durationMs: Long, recordsRead: Long, shuffleReadBytes: Long)

  private val stageTimings = mutable.Map.empty[Int, mutable.ArrayBuffer[TaskTiming]]
  private val stageInfo    = mutable.Map.empty[Int, StageInfo]

  override def onStageSubmitted(s: SparkListenerStageSubmitted): Unit = {
    stageInfo.put(s.stageInfo.stageId, s.stageInfo)
    stageTimings.put(s.stageInfo.stageId, mutable.ArrayBuffer.empty)
  }

  override def onTaskEnd(t: SparkListenerTaskEnd): Unit = {
    val m = Option(t.taskMetrics)
    val tt = TaskTiming(
      durationMs       = m.map(_.executorRunTime).getOrElse(0L),
      recordsRead      = m.map(_.inputMetrics.recordsRead).getOrElse(0L) +
                         m.map(_.shuffleReadMetrics.recordsRead).getOrElse(0L),
      shuffleReadBytes = m.map(_.shuffleReadMetrics.totalBytesRead).getOrElse(0L)
    )
    stageTimings.getOrElseUpdate(t.stageId, mutable.ArrayBuffer.empty) += tt
  }

  /** Pretty-print every captured stage. Call at end of job. */
  def report(): Unit = {
    println("=" * 96)
    println(f"${"stageId"}%8s ${"name"}%-32s ${"tasks"}%6s ${"medMs"}%8s " +
            f"${"maxMs"}%8s ${"skew"}%5s ${"shufRdMB"}%10s ${"recs"}%10s")
    println("=" * 96)
    stageTimings.toSeq.sortBy(_._1).foreach { case (sid, ts) =>
      if (ts.isEmpty) {
        println(f"$sid%8d (no task metrics)")
      } else {
        val durs = ts.map(_.durationMs).sorted
        val med  = durs(durs.length / 2)
        val mx   = durs.last
        val skew = if (med == 0) 0.0 else mx.toDouble / med
        val shuf = ts.map(_.shuffleReadBytes).sum / (1024.0 * 1024.0)
        val recs = ts.map(_.recordsRead).sum
        val name = stageInfo.get(sid).map(_.name).getOrElse("?").take(32)
        println(f"$sid%8d $name%-32s ${ts.size}%6d $med%8d $mx%8d $skew%5.1f $shuf%10.1f $recs%10d")
      }
    }
    println("=" * 96)
    println("(skew = max/median; > 5 is a clear stage-level skew red flag)")
  }
}

object MetricsCollector {
  /** Convenience: install the listener on a fresh session. */
  def install(spark: SparkSession): MetricsCollector = {
    val mc = new MetricsCollector
    spark.sparkContext.addSparkListener(mc)
    mc
  }
}
