import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, DataFrame, Encoder, SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{date_add, date_format, coalesce, col, concat_ws, lit, lower, max, regexp_replace, trim, to_date}

import scala.util.Try

object users_items extends App with Logging {
  implicit lazy val spark: SparkSession = SparkSession.builder
    .appName("Sergey Vyun Lab05")
    .config("spark.driver.cores", 1)
    .config("spark.driver.memory", "4g")
    .config("spark.driver.maxResultSize", "1g")
    .config("spark.executor.instances", 5)
    .config("spark.executor.cores", 2)
    .config("spark.executor.memory", "4g")
    .config("spark.default.parallelism", 10)
    .config("spark.sql.shuffle.partitions",10)
    .getOrCreate

  val hdfsInputPath: String = spark.conf.get("spark.users_items.input_dir", "/user/sergey.vyun/visits")
  val hdfsOutputPath: String = spark.conf.get("spark.users_items.output_dir", "/user/sergey.vyun/users-items")
  val modeFlag: Int = Try(spark.conf.get("spark.users_items.update", "0").toInt).toOption.getOrElse(0)

  spark.sparkContext.setLogLevel("INFO")
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  import spark.implicits._

  logInfo(s"[LAB05] Spark version: ${spark.version}")
  logInfo(s"[LAB05] HDFS Input path: $hdfsInputPath")
  logInfo(s"[LAB05] HDFS Output path: $hdfsOutputPath")
  logInfo(s"[LAB05] mode (0 - insert, 1 - update): $modeFlag")

  val schema0: StructType = StructType(
    StructField("category", StringType) ::
    StructField("date", StringType) ::
    StructField("event_type", StringType) ::
    StructField("item_id", StringType) ::
    StructField("item_price", LongType) ::
    StructField("timestamp", LongType) ::
    StructField("uid", StringType) ::
    StructField("p_date", IntegerType) :: Nil
  )

  val schema1: StructType = StructType(
    StructField("category", StringType) ::
    StructField("event_type", StringType) ::
    StructField("item_id", StringType) ::
    StructField("item_price", LongType) ::
    StructField("timestamp", LongType) ::
    StructField("uid", StringType) ::
    StructField("date", StringType) :: Nil
  )

  val logUid = "03001878-d923-4880-9c69-8b6884c7ad0e"

  val viewsSrc: DataFrame =
    if (modeFlag == 0) {
      val viewsPath = hdfsInputPath + "/view"
      logInfo(s"[LAB05] Reading Views from path: $viewsPath")
      read(viewsPath, "json", Some(schema0))
    }
    else {
      val viewsPath = hdfsInputPath + "/view/20200430"
      logInfo(s"[LAB05] Reading Views from path: $viewsPath")
      read(viewsPath, "json", Some(schema1))
    }
  logInfoStatistics(viewsSrc, "Views Source", logUid)

  val views: DataFrame = viewsSrc
    .withColumn("item_id", clearItemId("view", col("item_id")))
  logInfoStatistics(views, "Views", logUid)

  val buysSrc: DataFrame =
    if (modeFlag == 0) {
      val buysPath = hdfsInputPath + "/buy"
      logInfo(s"[LAB05] Reading Buys from path: $buysPath")
      read(buysPath, "json", Some(schema0))
    }
    else {
      val buysPath = hdfsInputPath + "/buy/20200430"
      logInfo(s"[LAB05] Reading Buys from path: $buysPath")
      read(buysPath, "json", Some(schema1))
    }
  logInfoStatistics(buysSrc, "Buys Source", logUid)

  val buys: DataFrame = buysSrc
    .withColumn("item_id", clearItemId("buy", col("item_id")))
  logInfoStatistics(buys, "Buys", logUid)

  val p_date: String = maxNum(
    maxDate[String](views, col("date")),
    maxDate[String](buys, col("date"))
  )
  logInfo(s"[LAB05] Partition date: $p_date")

  val p_prev_date: String = Seq(p_date).toDF("date")
    .select(
      date_format(date_add(to_date(col("date"), "yyyyMMdd"), -1), "yyyyMMdd").as[String]
    ).first
  logInfo(s"[LAB05] Partition Prev date: $p_prev_date")

  val usersItems: DataFrame = views.groupBy('uid, 'item_id).count
    .union(buys.groupBy('uid, 'item_id).count)
    .select('uid, 'item_id, 'count)
  logInfoStatistics(usersItems, "Users x Items", logUid)

  val usersItemsPivot: DataFrame = usersItems
    .groupBy('uid)
    .pivot('item_id)
    .agg(max('count))
  logInfoStatistics(usersItemsPivot, "Users x Items Pivot", logUid)

  val usersItemsRes: DataFrame = usersItemsPivot
    .select(
      usersItemsPivot.columns.map {
        case x if x.startsWith("buy_") || x.startsWith("view_") => coalesce(col(x), lit(0)).as(x)
        case y => col(y)
      }: _*
    ).repartition(1)
  logInfoStatistics(usersItemsRes, "Users x Items Result", logUid)

  val usersItemsPrev: DataFrame =
    if (modeFlag == 0) spark.emptyDataFrame
    else read(hdfsOutputPath + s"/$p_prev_date", "parquet")
  logInfoStatistics(usersItemsPrev, "Users x Items Prev", logUid)

  logInfo(s"[LAB05] Users x Items Prev columns:\n${usersItemsPrev.columns.mkString("\n")}\n")
  logInfo(s"[LAB05] Users x Items Result columns:\n${usersItemsRes.columns.mkString("\n")}\n")

  val result: DataFrame =
    if (modeFlag == 0) usersItemsRes
    else {
      usersItemsPrev
        .union {
          usersItemsRes
            .select(
              usersItemsPrev.columns.map { colNm =>
                if (usersItemsRes.columns.contains(colNm)) col(colNm).as(colNm)
                else lit(0).as(colNm)
              }: _*
            )
        }
    }

  write(result, hdfsOutputPath + s"/$p_date", "parquet")

  def logInfoStatistics(df: DataFrame,
                        name: String,
                        uid: String = "",
                        logFunction: String => Unit = str => logInfo(str)): Unit = {
    logFunction(s"[LAB05] $name count: ${df.count}")
    logFunction(s"[LAB05] $name schema:\n${df.schema.treeString}")
    logFunction(s"[LAB05] $name sample:\n${df.take(10).mkString("\n")}\n")
    logFunction(s"[LAB05] $name sample uid = '$uid':\n${takeByUid(df, uid)}\n")
  }

  def takeByUid(df: DataFrame, uid: String, rows: Int = 100): String = {
    if (df.columns.map(_.trim.toLowerCase).contains("uid"))
      df.filter(col("uid") === uid).take(rows).mkString("\n")
    else ""
  }

  def read(path: String,
           format: String,
           schema: Option[StructType] = None)
          (implicit spark: SparkSession): DataFrame = {
    val reader = schema
      .map(spark.read
        .format(format)
        .schema(_)
      )
      .getOrElse(spark.read
        .format(format)
      )

    reader.load(path)
  }

  def clearItemId(prefix: String, itemId: Column): Column = {
    concat_ws("_", lit(prefix), regexp_replace(lower(trim(itemId)), "( |-)", "_"))
  }

  def maxDate[A](df: DataFrame, col: Column)(implicit E: Encoder[A]): A = {
    df.select(max(col).as[A]).first
  }

  def maxNum[A](a: A, b: A)(implicit O: Ordering[A]): A = {
    if (O.compare(a, b) > 0) a else b
  }

  def write(df: DataFrame,
            path: String,
            format: String,
            mode: SaveMode = SaveMode.Overwrite): Unit = {
    df.write
      .format(format)
      .mode(mode)
      .save(path)
  }
}
