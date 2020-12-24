import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, DataFrame, DataFrameWriter, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, date_add, date_format, from_json, from_unixtime, max, struct, to_date, to_json}

import scala.util.{Success, Try}

object filter extends App with Logging {
  implicit lazy val spark: SparkSession = SparkSession.builder
    .appName("Sergey Vyun Lab04a")
    .config("spark.driver.cores", 1)
    .config("spark.driver.memory", "4g")
    .config("spark.driver.maxResultSize", "1g")
    .config("spark.executor.instances", 5)
    .config("spark.executor.cores", 2)
    .config("spark.executor.memory", "4g")
    .config("spark.default.parallelism", 10)
    .config("spark.sql.shuffle.partitions",10)
    .getOrCreate

  lazy val kafkaHosts = spark.conf.get("spark.filter.hosts", "spark-master-1:6667")
  lazy val kafkaTopic = spark.conf.get("spark.filter.topic_name", "lab04_input_data")
  lazy val kafkaStartingOffsets =
    parseOffset(
      spark.conf.get("spark.filter.offset", "earliest"),
      kafkaTopic
    )

  lazy val hdfsResultDirPrefix = spark.conf.get("spark.filter.output_dir_prefix", "/user/sergey.vyun/visits")
  lazy val homeResultDirPrefix = "file:///data/home/sergey.vyun/visits2/"

  def parseOffset(offset: String, topic: String): String = {
    Try(offset.toInt) match {
      case Success(i) => s"""{"$topic":{"0":$i}}"""
      case _ => offset
    }
  }

  spark.sparkContext.setLogLevel("INFO")
  import spark.implicits._

  logInfo(s"[LAB04A] Spark version: ${spark.version}")
  logInfo(s"[LAB04A] Kafka hosts: $kafkaHosts")
  logInfo(s"[LAB04A] Kafka topic: $kafkaTopic")
  logInfo(s"[LAB04A] Kafka starting offsets: $kafkaStartingOffsets")
  logInfo(s"[LAB04A] HDFS result dir prefix: $hdfsResultDirPrefix")

  val logUid = "5cafa3c48fa46dca527c4b6471795b76"

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
    .distinct
    .select(
      col("json.category").as("category"),
      col("json.event_type").as("event_type"),
      col("json.item_id").as("item_id"),
      col("json.item_price").as("item_price"),
      col("json.timestamp").as("timestamp"),
      col("json.uid").as("uid"),
      convertDate(col("json.timestamp")).as("date")
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
      col("uid"),
      col("date").as("p_date"),
      col("event_type")
    )
    .repartition(col("p_date"))
    .cache
  logInfoStatistics(df, "Data", logUid)

  val p_date: DataFrame = df
    .select(
      max(col("p_date")).as("max_p_date")
    )
    .limit(1)
    .cache
  logInfoStatistics(p_date, "Partition Date", logUid)

  val max_p_date: Int = p_date
    .select(col("max_p_date").as[Int])
    .first
  logInfo(s"[LAB04A] Partition Date: $max_p_date")

  val views: DataFrame = df.filter(col("event_type") === "view")
  logInfoStatistics(views, "Views", logUid)

  logInfo(s"[LAB04A] Saving Views to path: $hdfsResultDirPrefix/view")
  write(
    df = views.select(col("value"), col("p_date")).repartition(1),
    path = s"$hdfsResultDirPrefix/view/$max_p_date"//,
    //partitionBy = Seq("p_date")
  )
/*
  logInfo(s"[LAB04A] Saving Views to Home path: $homeResultDirPrefix/view")
  write(
    df = views.select(col("value"), col("p_date")),
    path = s"$homeResultDirPrefix/view",
    partitionBy = Seq("p_date")
  )
*/
  val buys: DataFrame = df.filter(col("event_type") === "buy")
  logInfoStatistics(buys, "Buys", logUid)

  logInfo(s"[LAB04A] Saving Buys to path: $hdfsResultDirPrefix/buy")
  write(
    df = buys.select(col("value"), col("p_date")).repartition(1),
    path = s"$hdfsResultDirPrefix/buy/$max_p_date"//,
    //partitionBy = Seq("p_date")
  )
/*
  logInfo(s"[LAB04A] Saving Buys to Home path: $homeResultDirPrefix/buy")
  write(
    df = buys.select(col("value"), col("p_date")),
    path = s"$homeResultDirPrefix/buy",
    partitionBy = Seq("p_date")
  )
*/
  def convertDate(unixTimestamp: Column): Column = {
    //date_format(to_date(from_unixtime(unixTimestamp / 1000)), "yyyyMMdd")
    date_format(date_add(to_date(from_unixtime(unixTimestamp / 1000)), -1), "yyyyMMdd")
  }

  def logInfoStatistics(df: DataFrame,
                        name: String,
                        uid: String = "",
                        logFunction: String => Unit = str => logInfo(str)): Unit = {
    logFunction(s"[LAB04A] $name count: ${df.count}")
    logFunction(s"[LAB04A] $name schema:\n${df.schema.treeString}")
    logFunction(s"[LAB04A] $name sample:\n${df.take(10).mkString("\n")}\n")
    logFunction(s"[LAB04A] $name sample uid = '$uid':\n${takeByUid(df, uid)}\n")
  }

  def takeByUid(df: DataFrame, uid: String, rows: Int = 100): String = {
    if (df.columns.map(_.trim.toLowerCase).contains("uid"))
      df.filter(col("uid") === uid).take(rows).mkString("\n")
    else ""
  }

  def write(df: DataFrame,
            path: String,
            partitionBy: Seq[String] = Nil,
            format: String = "text",
            mode: SaveMode = SaveMode.Overwrite): Unit = {
    val writer: DataFrameWriter[Row] = if (partitionBy.nonEmpty)
      df.write
        .format(format)
        .mode(mode)
        .partitionBy(partitionBy: _*)
    else
      df.write
        .format(format)
        .mode(mode)

    writer
      .save(path)
  }
}
