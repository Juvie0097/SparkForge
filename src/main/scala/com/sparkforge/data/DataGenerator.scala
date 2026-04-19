package com.sparkforge.data

import com.sparkforge.config.AppConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * Generates skewed synthetic transaction data and dimension tables.
 *
 * Skew pattern: `hotUserCount` users account for `hotUserFraction` of total
 * volume (Zipf-like). This reliably triggers skew in groupBy/join benchmarks.
 */
object DataGenerator {

  def generateAll(spark: SparkSession): Unit = {
    println("[DataGenerator] generating dimension tables…")
    generateDimUsers(spark)
    generateDimMerchants(spark)
    generateDimDevices(spark)
    generateDimMcc(spark)
    println("[DataGenerator] generating fact_transactions…")
    generateFacts(spark)
    println("[DataGenerator] done.")
  }

  // ---------------------------------------------------------------------------
  // Dimension tables
  // ---------------------------------------------------------------------------

  def generateDimUsers(spark: SparkSession): Unit = {
    import spark.implicits._
    val n = AppConfig.generator.numUsers
    spark.range(n)
      .select(
        col("id").as("user_id"),
        concat(lit("user_"), col("id")).as("user_name"),
        (col("id") % 5).cast("string").as("segment")
      )
      .write.mode(SaveMode.Overwrite)
      .parquet(AppConfig.data.rawPath + "/dim_users")
  }

  def generateDimMerchants(spark: SparkSession): Unit = {
    val n = AppConfig.generator.numMerchants
    spark.range(n)
      .select(
        col("id").as("merchant_id"),
        concat(lit("merchant_"), col("id")).as("merchant_name"),
        (col("id") % 100).as("mcc")
      )
      .write.mode(SaveMode.Overwrite)
      .parquet(AppConfig.data.rawPath + "/dim_merchants")
  }

  def generateDimDevices(spark: SparkSession): Unit = {
    val n = AppConfig.generator.numDevices
    spark.range(n)
      .select(
        col("id").as("device_id").cast("string"),
        concat(lit("device_"), col("id")).as("device_name"),
        (col("id") % 3).cast("string").as("platform")
      )
      .write.mode(SaveMode.Overwrite)
      .parquet(AppConfig.data.rawPath + "/dim_devices")
  }

  def generateDimMcc(spark: SparkSession): Unit = {
    spark.range(100)
      .select(
        col("id").as("mcc"),
        concat(lit("category_"), col("id")).as("mcc_desc")
      )
      .write.mode(SaveMode.Overwrite)
      .parquet(AppConfig.data.rawPath + "/dim_mcc")
  }

  // ---------------------------------------------------------------------------
  // Fact table — skewed by design
  // ---------------------------------------------------------------------------

  def generateFacts(spark: SparkSession): Unit = {
    val cfg          = AppConfig.generator
    val numTxn       = cfg.numTransactions
    val numUsers     = cfg.numUsers
    val numMerchants = cfg.numMerchants
    val numDevices   = cfg.numDevices
    val hotCount     = cfg.hotUserCount
    val hotFraction  = cfg.hotUserFraction
    val daysBack     = cfg.daysBack
    val nullRate     = cfg.nullDeviceRate
    val partitions   = cfg.rawWritePartitions

    val df = spark.range(numTxn)
      .repartition(partitions)
      .select(
        col("id").as("txn_id"),
        when(rand() < hotFraction,
             (rand() * hotCount).cast("long"))
          .otherwise(
             (lit(hotCount) + (rand() * (numUsers - hotCount)).cast("long")))
          .as("user_id"),
        (rand() * numMerchants).cast("long").as("merchant_id"),
        when(rand() < nullRate, lit(null).cast("string"))
          .otherwise((rand() * numDevices).cast("long").cast("string"))
          .as("device_id"),
        concat(
          (lit(10) + (rand() * 245).cast("int")).cast("string"), lit("."),
          (rand() * 255).cast("int").cast("string"),             lit("."),
          (rand() * 255).cast("int").cast("string"),             lit("."),
          (rand() * 255).cast("int").cast("string")
        ).as("ip"),
        concat(
          element_at(array((0 until 32).map(i => lit(('a' + i % 26).toChar.toString)): _*),
                     (rand() * 32).cast("int") + 1),
          element_at(array((0 until 32).map(i => lit(('a' + i % 26).toChar.toString)): _*),
                     (rand() * 32).cast("int") + 1),
          element_at(array((0 until 32).map(i => lit(('0' + i % 10).toChar.toString)): _*),
                     (rand() * 32).cast("int") + 1),
          element_at(array((0 until 32).map(i => lit(('a' + i % 26).toChar.toString)): _*),
                     (rand() * 32).cast("int") + 1),
          element_at(array((0 until 32).map(i => lit(('0' + i % 10).toChar.toString)): _*),
                     (rand() * 32).cast("int") + 1)
        ).as("geo_hash"),
        exp(rand() * 5).cast("double").as("amount"),
        element_at(array(lit("online"), lit("pos"), lit("atm"), lit("mobile")),
                   (rand() * 4).cast("int") + 1).as("channel"),
        element_at(array(lit("approved"), lit("approved"), lit("approved"), lit("declined"), lit("error")),
                   (rand() * 5).cast("int") + 1).as("status"),
        (unix_timestamp(current_timestamp()) - (rand() * daysBack * 86400).cast("long"))
          .cast("timestamp").as("ts")
      )
      .withColumn("dt", to_date(col("ts")))

    df.write
      .mode(SaveMode.Overwrite)
      .partitionBy("dt")
      .option("compression", "snappy")
      .parquet(AppConfig.data.rawPath + "/fact_transactions")
  }
}
