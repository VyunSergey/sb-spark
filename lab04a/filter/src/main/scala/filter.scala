import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, date_format, from_json, from_unixtime, struct, to_date, to_json}

import scala.util.{Success, Try}

object filter extends App with Logging {
  lazy val spark: SparkSession = SparkSession.builder.getOrCreate

  lazy val kafkaHosts = spark.conf.get("spark.filter.hosts", "spark-master-1:6667")
  lazy val kafkaTopic = spark.conf.get("spark.filter.topic_name", "lab04_input_data")
  lazy val kafkaStartingOffsets =
    parseOffset(
      spark.conf.get("spark.filter.offset", "earliest"),
      kafkaTopic
    )

  lazy val hdfsResultDirPrefix = spark.conf.get("spark.filter.output_dir_prefix", "/user/sergey.vyun/visits")

  def parseOffset(offset: String, topic: String): String = {
    Try(offset.toInt) match {
      case Success(i) => s"""{"$topic":{"0":$i}}"""
      case _ => offset
    }
  }

  spark.sparkContext.setLogLevel("INFO")

  logInfo(s"[LAB04A] Spark version: ${spark.version}")
  logInfo(s"[LAB04A] Kafka hosts: $kafkaHosts")
  logInfo(s"[LAB04A] Kafka topic: $kafkaTopic")
  logInfo(s"[LAB04A] Kafka starting offsets: $kafkaStartingOffsets")
  logInfo(s"[LAB04A] HDFS result dir prefix: $hdfsResultDirPrefix")

  val schema: StructType = StructType(
    StructField("event_type", StringType) ::
      StructField("category", StringType) ::
      StructField("item_id", StringType) ::
      StructField("item_price", LongType) ::
      StructField("uid", StringType) ::
      StructField("timestamp", LongType) :: Nil
  )

  val df: DataFrame = spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", kafkaHosts)
    .option("subscribe", kafkaTopic)
    .option("startingOffsets", kafkaStartingOffsets)
    .load
    .select(
      from_json(col("value").cast(StringType), schema).as("json")
    )
    .select(
      col("json.category").as("category"),
      col("json.event_type").as("event_type"),
      col("json.item_id").as("item_id"),
      col("json.item_price").as("item_price"),
      col("json.timestamp").as("timestamp"),
      col("json.uid").as("uid"),
      date_format(to_date(from_unixtime(col("json.timestamp") / 1000)), "yyyyMMdd").as("date")
    )
    .select(
      to_json(
        struct(
          col("category"),
          col("event_type"),
          col("item_id"),
          col("item_price"),
          col("timestamp"),
          col("uid"),
          col("date")
        )
      ).as("value"),
      col("date").as("p_date"),
      col("event_type")
    )
    .repartition(col("p_date"))

  df.filter(col("event_type") === "view")
    .select(
      col("value"),
      col("p_date")
    )
    .write
    .mode(SaveMode.Overwrite)
    .partitionBy("p_date")
    .text(s"$hdfsResultDirPrefix/view")

  df.filter(col("event_type") === "buy")
    .select(
      col("value"),
      col("p_date")
    )
    .write
    .mode(SaveMode.Overwrite)
    .partitionBy("p_date")
    .text(s"$hdfsResultDirPrefix/buy")
}