package com.sparkforge.util

import com.sparkforge.config.AppConfig
import org.apache.spark.sql.DataFrame

import java.io.PrintWriter
import java.nio.file.{Files, Paths}

/**
 * Drops the `explain(mode="formatted")` output of a DataFrame to disk, so we
 * can review plans across runs (compare AQE on/off, salting on/off) without
 * scrolling through console logs.
 *
 * Files are written under `<base-path>/_plans/<name>.txt`. Invoke around
 * critical join points and aggregate boundaries.
 */
object ExplainHelper {

  private lazy val plansDir = {
    val p = Paths.get(AppConfig.data.basePath, "_plans")
    Files.createDirectories(p)
    p
  }

  def dumpPlan(df: DataFrame, name: String, mode: String = "formatted"): Unit = {
    val rendered = captureExplain(df, mode)
    val target   = plansDir.resolve(s"$name.txt")
    val w = new PrintWriter(target.toFile)
    try {
      w.println(s"==== plan: $name (mode=$mode) ====")
      w.println(rendered)
    } finally w.close()
    println(s"[explain] wrote $target")
  }

  /** Capture explain output as a String — Spark prints to stdout natively. */
  private def captureExplain(df: DataFrame, mode: String): String = {
    val baos = new java.io.ByteArrayOutputStream()
    val ps   = new java.io.PrintStream(baos, true, "UTF-8")
    val orig = System.out
    System.setOut(ps)
    try { df.explain(mode) } finally { System.setOut(orig) }
    baos.toString("UTF-8")
  }
}
