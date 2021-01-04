import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.util.Try

object features  extends App with Logging {
  implicit lazy val spark: SparkSession = SparkSession.builder
    .appName("Sergey Vyun Lab06")
    .config("spark.driver.cores", 1)
    .config("spark.driver.memory", "4g")
    .config("spark.driver.maxResultSize", "1g")
    .config("spark.executor.instances", 5)
    .config("spark.executor.cores", 2)
    .config("spark.executor.memory", "4g")
    .config("spark.default.parallelism", 10)
    .config("spark.sql.shuffle.partitions",10)
    .getOrCreate

  val hdfsInputPath: String = spark.conf.get("spark.features.users_items_dir", "/user/sergey.vyun/users-items/20200429")
  val jsonInputPath: String = spark.conf.get("spark.features.weblogs_dir", "/labs/laba03/weblogs.json")
  val hdfsOutputPath: String = spark.conf.get("spark.features.output", "/user/sergey.vyun/features/")
  val logUID: String = "d50192e5-c44e-4ae8-ae7a-7cfe67c8b777"

  spark.conf.set("spark.sql.session.timeZone", "UTC")
  spark.sparkContext.setLogLevel("INFO")
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  logInfo(s"[LAB06] Spark version: ${spark.version}")
  logInfo(s"[LAB06] HDFS Input path: $hdfsInputPath")
  logInfo(s"[LAB06] JSON Input path: $jsonInputPath")
  logInfo(s"[LAB06] HDFS Output path: $hdfsOutputPath")

  val decoder: String => Option[String] = (url: String) =>
    Try(new java.net.URL(java.net.URLDecoder.decode(url)).getHost).toOption
  val decoderUdf: UserDefinedFunction = udf(decoder)

  val webLogs: DataFrame = spark
    .read
    .format("json")
    .option("inferSchema", "false")
    .load(jsonInputPath)
    .filter(col("uid").isNotNull)
    .filter(col("visits").isNotNull)
    .select(
      col("uid"),
      explode_outer(col("visits")).as("visits")
    )
    .select(
      col("uid"),
      lower(trim(decoderUdf(
        regexp_replace(
          regexp_replace(
            regexp_replace(col("visits.url"), "(http(s)?:\\/\\/https(:)?\\/\\/)", "https:\\/\\/"),
            "(http(s)?:\\/\\/http(:)?\\/\\/)", "http:\\/\\/"),
          "www\\.", "")
      ))).as("domain"),
      col("visits.url").as("url"),
      from_unixtime(col("visits.timestamp") / 1000).as("timestamp")
    )
  logInfoStatistics(webLogs, "Weblogs", logUID)

  val groupWebLogs: DataFrame = webLogs
    .groupBy(col("domain"))
    .count
    .withColumn("rn", row_number().over(Window.orderBy(col("count").desc, col("domain"))))
    .filter(col("rn") <= 1000)
  logInfoStatistics(groupWebLogs, "GroupWeblogs", logUID)

  val topWebLogs: DataFrame = webLogs
    .join(broadcast(groupWebLogs), Seq("domain"), "inner")
    .orderBy(col("domain"), col("uid"))
    .cache
  logInfoStatistics(topWebLogs, "TopWeblogs", logUID)

  val domainWebLogs: DataFrame = topWebLogs
    .groupBy(col("uid"))
    .pivot(col("domain"))
    .agg(sum(lit(1L)))
  logInfoStatistics(domainWebLogs, "DomainWeblogs", logUID)

  val vectorWebLogs: DataFrame = domainWebLogs
    .select(
      col("uid"),
      array(
        domainWebLogs.columns.filterNot(_ == "uid").map { colNm =>
          coalesce(col(s"`$colNm`"), lit(0L)).as(colNm)
        }: _*
      ).as("domain_features")
    )
  logInfoStatistics(vectorWebLogs, "VectorWeblogs", logUID)

  val weekWebLogs: DataFrame = topWebLogs
    .select(
      col("uid"),
      toDayOfWeek(dayofweek(col("timestamp"))).as("week")
    )
    .groupBy(col("uid"))
    .pivot(col("week"))
    .agg(sum(lit(1L)))
  logInfoStatistics(weekWebLogs, "WeekWeblogs", logUID)

  val hourWebLogs: DataFrame = topWebLogs
    .select(
      col("uid"),
      toHourOfDay(hour(col("timestamp"))).as("hour")
    )
    .groupBy(col("uid"))
    .pivot(col("hour"))
    .agg(sum(lit(1L)))
  logInfoStatistics(hourWebLogs, "HourWeblogs", logUID)

  val fractionWebLogs: DataFrame = topWebLogs
    .select(
      col("uid"),
      toWorkHours(hour(col("timestamp"))).as("work_hours"),
      toEveningHours(hour(col("timestamp"))).as("evening_hours")
    )
    .groupBy(col("uid"))
    .agg(
      (sum(col("work_hours")).cast(DoubleType) / sum(lit(1L)).cast(DoubleType)).as("web_fraction_work_hours"),
      (sum(col("evening_hours")).cast(DoubleType) / sum(lit(1L)).cast(DoubleType)).as("web_fraction_evening_hours")
    )
  logInfoStatistics(fractionWebLogs, "FractionWeblogs", logUID)

  val resultWebLogs: DataFrame = vectorWebLogs
    .join(weekWebLogs
      .select(
        col("uid") +:
        weekWebLogs.columns.filterNot(_ == "uid").map { colNm =>
          coalesce(col(s"`$colNm`"), lit(0L)).as(colNm)
        }: _*), Seq("uid"), "left")
    .join(hourWebLogs
      .select(
        col("uid") +:
          hourWebLogs.columns.filterNot(_ == "uid").map { colNm =>
            coalesce(col(s"`$colNm`"), lit(0L)).as(colNm)
          }: _*), Seq("uid"), "left")
    .join(fractionWebLogs, Seq("uid"), "left")
  logInfoStatistics(resultWebLogs, "ResultWeblogs", logUID)

  val userItems: DataFrame = spark
    .read
    .format("parquet")
    .load(hdfsInputPath)
  logInfoStatistics(userItems, "UserItems", logUID)

  val result: DataFrame = resultWebLogs
    .join(userItems, Seq("uid"), "left")
  logInfoStatistics(result, "Result", logUID)

  result
    .repartition(1)
    .write
    .format("parquet")
    .mode(SaveMode.Overwrite)
    .save(hdfsOutputPath)

  def toDayOfWeek(column: Column): Column = {
    when(column === 1, "web_day_sun")
      .when(column === 2, "web_day_mon")
      .when(column === 3, "web_day_tue")
      .when(column === 4, "web_day_wed")
      .when(column === 5, "web_day_thu")
      .when(column === 6, "web_day_fri")
      .when(column === 7, "web_day_sat")
      .otherwise("web_day_sun")
  }

  def toHourOfDay(column: Column): Column = {
    concat(lit("web_hour_"), column.cast(StringType))
  }

  def toWorkHours(column: Column): Column = {
    when(column.between(9, 17), 1L)
      .otherwise(0L)
  }

  def toEveningHours(column: Column): Column = {
    when(column.between(18, 23), 1L)
      .otherwise(0L)
  }

  def logInfoStatistics(df: DataFrame,
                        name: String,
                        uid: String = "",
                        logFunction: String => Unit = str => logInfo(str)): Unit = {
    logFunction(s"[LAB06] $name count: ${df.count}")
    logFunction(s"[LAB06] $name schema:\n${df.schema.treeString}")
    logFunction(s"[LAB06] $name sample:\n${df.take(10).mkString("\n")}\n")
    logFunction(s"[LAB06] $name sample uid = '$uid':\n${takeByUid(df, uid)}\n")
  }

  def takeByUid(df: DataFrame, uid: String, rows: Int = 100): String = {
    if (df.columns.map(_.trim.toLowerCase).contains("uid"))
      df.filter(col("uid") === uid).take(rows).mkString("\n")
    else ""
  }
}
