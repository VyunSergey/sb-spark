import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, DataFrame, Encoder, SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{coalesce, col, concat_ws, lit, lower, max, regexp_replace, trim}

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

  val hdfsInputPath = spark.conf.get("spark.users_items.input_dir", "/user/sergey.vyun/visits")
  val hdfsOutPutPath = spark.conf.get("spark.users_items.output_dir", "/user/sergey.vyun/users-items")
  val modeFlag = spark.conf.get("spark.users_items.update", "0")

  spark.sparkContext.setLogLevel("INFO")

  import spark.implicits._

  logInfo(s"[LAB05] Spark version: ${spark.version}")
  logInfo(s"[LAB05] HDFS Input path: $hdfsInputPath")
  logInfo(s"[LAB05] HDFS Input path: $hdfsOutPutPath")
  logInfo(s"[LAB05] mode (0 - insert, 1 - update): $modeFlag")

  val schema: StructType = StructType(
    StructField("category", StringType) ::
    StructField("date", StringType) ::
    StructField("event_type", StringType) ::
    StructField("item_id", StringType) ::
    StructField("item_price", LongType) ::
    StructField("timestamp", LongType) ::
    StructField("uid", StringType) ::
    StructField("p_date", IntegerType) :: Nil
  )

  val views: DataFrame = read(hdfsInputPath + "/view", "json", Some(schema))
    .withColumn("item_id", clearItemId("view", col("item_id")))

  val buys: DataFrame = read(hdfsInputPath + "/buy", "json", Some(schema))
    .withColumn("item_id", clearItemId("buy", col("item_id")))

  val p_date: Int = maxNum(
    maxDate[Int](views, col("p_date")),
    maxDate[Int](buys, col("p_date"))
  )

  val usersItems: DataFrame = views.groupBy('uid, 'item_id).count
    .union(buys.groupBy('uid, 'item_id).count)
    .select('uid, 'item_id, 'count)

  val usersItemsPivot: DataFrame = usersItems
    .groupBy('uid)
    .pivot('item_id)
    .agg(max('count))

  val usersItemsRes: DataFrame = usersItemsPivot
    .select(
      usersItemsPivot.columns.map {
        case x if x.startsWith("buy_") || x.startsWith("view_") => coalesce(col(x), lit(0)).as(x)
        case y => col(y)
      }: _*
    ).repartition(1)

  write(usersItemsRes, hdfsOutPutPath + s"/$p_date", "parquet")

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
