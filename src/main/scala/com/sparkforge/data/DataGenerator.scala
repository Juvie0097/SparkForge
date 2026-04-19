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

  def generateAll(spark: SparkSession): Unit = {{

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
          // mcc foreign key → 0..99
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
          (col("id") % 3).cast("string").as("platform")   // 0=iOS, 1=Android, 2=Web
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

      // Hot users occupy ids 0..(hotCount-1).
      // If rand < hotFraction → pick from [0, hotCount); else pick from [hotCount, numUsers).
      val df = spark.range(numTxn)
        .repartition(partitions)
        .select(
          col("id").as("txn_id"),
          // skewed user_id
          when(rand() < hotFraction,
            (rand() * hotCount).cast("long"))
            .otherwise(
              (lit(hotCount) + (rand() * (numUsers - hotCount)).cast("long")))
            .as("user_id"),
          (rand() * numMerchants).cast("long").as("merchant_id"),
          // device_id: nullable String, null at nullRate
          when(rand() < nullRate, lit(null).cast("string"))
            .otherwise((rand() * numDevices).cast("long").cast("string"))
            .as("device_id"),
          // ip: synthetic v4
          concat(
            (lit(10) + (rand() * 245).cast("int")).cast("string"), lit("."),
            (rand() * 255).cast("int").cast("string"),             lit("."),
            (rand() * 255).cast("int").cast("string"),             lit("."),
            (rand() * 255).cast("int").cast("string")
          ).as("ip"),
          // geo_hash: 5-char prefix from a small alphabet
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
          // amount: log-normal-ish via exponential of uniform
          exp(rand() * 5).cast("double").as("amount"),
          // channel
          element_at(array(lit("online"), lit("pos"), lit("atm"), lit("mobile")),
            (rand() * 4).cast("int") + 1).as("channel"),
          // status
          element_at(array(lit("approved"), lit("approved"), lit("approved"), lit("declined"), lit("error")),
            (rand() * 5).cast("int") + 1).as("status"),
          // ts: random second within last daysBack days
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
        // mcc foreign key → 0..99
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
        (col("id") % 3).cast("string").as("platform")   // 0=iOS, 1=Android, 2=Web
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

    // Hot users occupy ids 0..(hotCount-1).
    // If rand < hotFraction → pick from [0, hotCount); else pick from [hotCount, numUsers).
    val df = spark.range(numTxn)
      .repartition(partitions)
      .select(
        col("id").as("txn_id"),
        // skewed user_id
        when(rand() < hotFraction,
             (rand() * hotCount).cast("long"))
          .otherwise(
             (lit(hotCount) + (rand() * (numUsers - hotCount)).cast("long")))
          .as("user_id"),
        (rand() * numMerchants).cast("long").as("merchant_id"),
        // device_id: nullable String, null at nullRate
        when(rand() < nullRate, lit(null).cast("string"))
          .otherwise((rand() * numDevices).cast("long").cast("string"))
          .as("device_id"),
        // ip: synthetic v4
        concat(
          (lit(10) + (rand() * 245).cast("int")).cast("string"), lit("."),
          (rand() * 255).cast("int").cast("string"),             lit("."),
          (rand() * 255).cast("int").cast("string"),             lit("."),
          (rand() * 255).cast("int").cast("string")
        ).as("ip"),
        // geo_hash: 5-char prefix from a small alphabet
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
        // amount: log-normal-ish via exponential of uniform
        exp(rand() * 5).cast("double").as("amount"),
        // channel
        element_at(array(lit("online"), lit("pos"), lit("atm"), lit("mobile")),
                   (rand() * 4).cast("int") + 1).as("channel"),
        // status
        element_at(array(lit("approved"), lit("approved"), lit("approved"), lit("declined"), lit("error")),
                   (rand() * 5).cast("int") + 1).as("status"),
        // ts: random second within last daysBack days
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
