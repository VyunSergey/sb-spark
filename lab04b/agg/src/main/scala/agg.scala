import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, from_json, from_unixtime, min, struct, sum, to_json, to_timestamp, unix_timestamp, when, window}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

import scala.concurrent.duration._
import scala.util.{Random, Try}

object agg extends App with Logging {
  lazy val spark: SparkSession = SparkSession.builder
    .appName("Sergey Vyun Lab04b")
    .config("spark.driver.cores", 1)
    .config("spark.driver.memory", "4g")
    .config("spark.driver.maxResultSize", "1g")
    .config("spark.executor.instances", 5)
    .config("spark.executor.cores", 2)
    .config("spark.executor.memory", "4g")
    .config("spark.default.parallelism", 10)
    .config("spark.sql.shuffle.partitions",10)
    .getOrCreate

  val kafkaHosts = spark.conf.get("spark.agg.kafka.hosts", "spark-master-1:6667")
  val kafkaInputTopic = spark.conf.get("spark.agg.kafka.input.topic", "sergey_vyun")
  val kafkaStartingOffsets = spark.conf.get("spark.agg.kafka.input.start.offset", "earliest")
  val kafkaMaxOffsetsPreTrigger =
    Try(spark.conf.get("spark.agg.kafka.input.max.offsets", "1000").toLong).getOrElse(1000L)
  val kafkaOutputTopic = spark.conf.get("spark.agg.kafka.output.topic", "sergey_vyun_lab04b_out")
  val dateTimeNow: String = LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy_MM_dd_hh_mm_ss"))
  val kafkaCheckPointLocation = spark.conf.get("spark.agg.kafka.checkpoint.location", s"/tmp/sergey.vyun/chk/lab04/state_${dateTimeNow}_${Random.nextInt(1000)}")

  spark.sparkContext.setLogLevel("INFO")
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  logInfo(s"[LAB04B] Spark version: ${spark.version}")
  logInfo(s"[LAB04B] Kafka hosts: $kafkaHosts")
  logInfo(s"[LAB04B] Kafka input topic: $kafkaInputTopic")
  logInfo(s"[LAB04B] Kafka starting offsets: $kafkaStartingOffsets")
  logInfo(s"[LAB04B] Kafka max offsets per triger: $kafkaMaxOffsetsPreTrigger")
  logInfo(s"[LAB04B] Kafka output topic: $kafkaOutputTopic")
  logInfo(s"[LAB04B] Kafka checkpoint location: $kafkaCheckPointLocation")

  val schema: StructType = StructType(
      StructField("event_type", StringType) ::
      StructField("category", StringType) ::
      StructField("item_id", StringType) ::
      StructField("item_price", LongType) ::
      StructField("uid", StringType) ::
      StructField("timestamp", LongType) :: Nil
  )

  val sdf: DataFrame = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafkaHosts)
    .option("subscribe", kafkaInputTopic)
    .option("startingOffsets", kafkaStartingOffsets)
    .option("maxOffsetsPerTrigger", kafkaMaxOffsetsPreTrigger)
    .load
    .select(
      from_json(col("value").cast(StringType), schema).as("json")
    )
    .select(
      col("json.category").as("category"),
      col("json.event_type").as("event_type"),
      col("json.item_price").as("item_price"),
      col("json.uid").as("uid"),
      to_timestamp(from_unixtime(col("json.timestamp").cast(LongType) / 1000)).as("timestamp")
    )
    .withWatermark("timestamp", "1 hour")
    .groupBy(
      window(col("timestamp"), "1 hour", "1 hour")
    )
    .agg(
      min(col("timestamp")).as("timestamp"),
      unix_timestamp(min(col("timestamp"))).as("start_ts"),
      (unix_timestamp(min(col("timestamp"))) + 60 * 60).as("end_ts"),
      sum(when(col("uid").isNotNull, 1L).otherwise(0L)).as("visitors"),
      sum(when(col("event_type") === "buy", 1L).otherwise(0L)).as("purchases"),
      sum(when(col("event_type") === "buy", col("item_price").cast(LongType)).otherwise(0L)).as("revenue")
    )
    .select(
      to_json(struct(
        col("start_ts"),
        col("end_ts"),
        col("revenue"),
        col("visitors"),
        col("purchases"),
        (col("revenue").cast(DoubleType) / col("purchases").cast(DoubleType)).as("aov")
      )).as("value")
    )

  sdf
    .writeStream
    .format("kafka")
    .outputMode(OutputMode.Update)
    .trigger(Trigger.ProcessingTime(30.seconds))
    .option("kafka.bootstrap.servers", kafkaHosts)
    .option("topic", kafkaOutputTopic)
    .option("checkpointLocation", kafkaCheckPointLocation)
    .start
    .awaitTermination(10.minutes.toMillis)
}
