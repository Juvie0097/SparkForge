package com.sparkforge

import com.sparkforge.advanced.{BucketJoinDemo, DppDemo}
import com.sparkforge.casebook._
import com.sparkforge.compact.{CompactJob, FileSizeAnalyzer}
import com.sparkforge.config.AppConfig
import com.sparkforge.data.DataGenerator
import com.sparkforge.features.FeaturePipeline
import com.sparkforge.tuning.{AqeConfigurator, CacheManager}
import com.sparkforge.util.MetricsCollector
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

/**
 * Single CLI entrypoint. Each `--stage` runs an independent step so that
 * benchmarks can isolate exactly what changed.
 *
 * Examples:
 *   spark-submit --class com.sparkforge.SparkForgeApp app.jar --stage gen
 *   spark-submit ... --stage features
 *   spark-submit ... --stage compact
 *   spark-submit ... --stage case --case nullkey
 *   spark-submit ... --stage advanced --demo dpp
 */
object SparkForgeApp {

  def main(args: Array[String]): Unit = {
    val argMap = parseArgs(args)
    val stage  = argMap.getOrElse("--stage", "all")
    val tuning = argMap.getOrElse("--tuning", "recommended")  // recommended | baseline | laptop

    val spark = buildSession()
    AppConfig.tuning   // touch to fail-fast on bad config
    tuning match {
      case "baseline"     => AqeConfigurator.applyBaseline(spark)
      case "laptop"       => AqeConfigurator.applyLocalLaptop(spark)
      case _              => AqeConfigurator.applyRecommended(spark)
    }
    AqeConfigurator.dumpAqeConfs(spark)

    val metrics = MetricsCollector.install(spark)

    try {
      stage match {
        case "gen"      => DataGenerator.generateAll(spark)
        case "features" => FeaturePipeline.runAndWrite(spark)
        case "compact"  => CompactJob.compactAll(spark)
        case "analyze"  =>
          val s = FileSizeAnalyzer.analyze(spark, AppConfig.data.rawPath + "/fact_transactions")
          println(s.pretty)
        case "case"     => runCase(spark, argMap.getOrElse("--case", "nullkey"))
        case "advanced" => runAdvanced(spark, argMap.getOrElse("--demo", "dpp"))
        case "all"      =>
          DataGenerator.generateAll(spark)
          FeaturePipeline.runAndWrite(spark)
          CompactJob.compactAll(spark)
        case other      => sys.error(s"unknown stage: $other")
      }
    } finally {
      metrics.report()
      CacheManager.unpersistAll()
      spark.stop()
    }
  }

  private def runCase(spark: SparkSession, name: String): Unit = {
    val factsPath = AppConfig.data.rawPath + "/fact_transactions"
    val devPath   = AppConfig.data.rawPath + "/dim_devices"
    val userPath  = AppConfig.data.rawPath + "/dim_users"

    name match {
      case "nullkey"  => NullKeySkewCase.demo(spark, factsPath, devPath)
      case "cast"     => ImplicitCastJoinCase.demo(spark, factsPath, userPath)
      case "twostage" => TwoStageAggCase.demo(spark, factsPath)
      case "udf"      => UdfPushdownCase.demo(spark, factsPath)
      case "window"   => WindowSkewCase.demo(spark, factsPath)
      case other      => sys.error(s"unknown case: $other")
    }
  }

  private def runAdvanced(spark: SparkSession, name: String): Unit = name match {
    case "dpp"    =>
      DppDemo.setUp(spark)
      DppDemo.runWithDpp(spark)
      DppDemo.runWithoutDpp(spark)
    case "bucket" =>
      BucketJoinDemo.materialise(spark)
      BucketJoinDemo.joinShuffleFree(spark)
    case other    => sys.error(s"unknown advanced demo: $other")
  }

  // -- session -----------------------------------------------------------

  private def buildSession(): SparkSession = {
    val conf = new SparkConf()
      .setAppName(AppConfig.appName)
      .set("spark.serializer",                 classOf[KryoSerializer].getName)
      .set("spark.kryo.registrationRequired", "false")
      .set("spark.sql.session.timeZone",      "UTC")
      // Allow this to work both with --master from the cluster submit script
      // and as a `sbt run` from a laptop.
      .setIfMissing("spark.master",            AppConfig.master)
      .setIfMissing("spark.sql.warehouse.dir", AppConfig.data.basePath + "/warehouse")

    SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
  }

  // -- arg parsing -------------------------------------------------------

  private def parseArgs(args: Array[String]): Map[String, String] = {
    args.sliding(2, 2).collect {
      case Array(k, v) if k.startsWith("--") => k -> v
    }.toMap
  }
}
