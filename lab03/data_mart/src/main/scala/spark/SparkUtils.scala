package spark

import domain.{Clients, ClientsCategory, DomainCategory, DomainWebCategory, OnlineLogs, ShopCategory, WebCategory, WebLogs}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

import scala.util.Try

object SparkUtils {
  def makeCategory(ds: Dataset[Clients])
                  (implicit spark: SparkSession): Dataset[ClientsCategory] = {
    import spark.implicits._

    ds
      .filter(ds("uid").isNotNull)
      .filter(ds("age") >= 18)
      .select(
        ds("uid"),
        ds("gender"),
        when(ds("age").between(18, 24), "18-24")
          .when(ds("age").between(25, 34), "25-34")
          .when(ds("age").between(35, 44), "35-44")
          .when(ds("age").between(45, 54), "45-54")
          .when(ds("age") >= 55, ">=55")
          .as("age_cat"),
        ds("age")
      )
      .as[ClientsCategory]
  }

  def makeShopCategory(ds: Dataset[OnlineLogs])
                      (implicit spark: SparkSession): Dataset[ShopCategory] = {
    import spark.implicits._

    ds
      .filter(ds("uid").isNotNull)
      .filter(ds("category").isNotNull)
      .select(
        ds("uid"),
        concat(lit("shop_"),
          regexp_replace(lower(trim(ds("category"))), "( |-)", "\\_")
        ).as("shop_cat")
      )
      .groupBy(col("uid"), col("shop_cat"))
      .count
      .select(
        col("uid"),
        col("shop_cat"),
        col("count").as("shop_cnt")
      )
      .as[ShopCategory]
  }

  def makeDomainWebCategory(ds: Dataset[DomainCategory])
                           (implicit spark: SparkSession): Dataset[DomainWebCategory] = {
    import spark.implicits._

    ds
      .filter(ds("domain").isNotNull)
      .filter(ds("category").isNotNull)
      .select(
        ds("domain"),
        concat(lit("web_"),
          regexp_replace(lower(trim(ds("category"))), "( |-)", "\\_")
        ).as("web_cat")
      )
      .as[DomainWebCategory]
  }

  def makeWebCategory(ds: Dataset[WebLogs], dic: Dataset[DomainWebCategory])
                     (implicit spark: SparkSession): Dataset[WebCategory] = {
    import spark.implicits._

    val decoder: String => Option[String] = (url: String) =>
      Try(new java.net.URL(java.net.URLDecoder.decode(url)).getHost).toOption
    val decoderUdf: UserDefinedFunction = udf(decoder)

    def clearUrl(url: Column): Column =
      regexp_replace(
        regexp_replace(
          regexp_replace(lower(trim(url)),
            "^http(s)?:\\/\\/(http(s)?:\\/\\/)?", "http://"),
          "^http:\\/\\/www\\.", "http://"),
        "www\\.", "")

    ds
      .filter(ds("uid").isNotNull)
      .filter(ds("visits").isNotNull)
      .select(
        ds("uid"),
        explode_outer(ds("visits")).as("visits")
      )
      .select(
        col("uid"),
        lower(trim(decoderUdf(clearUrl(col("visits.url"))))).as("domain"),
        col("visits.url").as("url")
      )
      .join(dic, Seq("domain"), "inner")
      .groupBy(col("uid"), col("web_cat"))
      .count
      .select(
        col("uid"),
        col("web_cat"),
        col("count").as("web_cnt")
      )
      .as[WebCategory]
  }

  def pivotDataFrame[T](ds: Dataset[T])
                       (groupColNm: String, pivotColNm: String, aggColNm: String)
                       (aggOp: Column => Column): DataFrame = {
    ds
      .groupBy(groupColNm)
      .pivot(pivotColNm)
      .agg(aggOp(col(aggColNm)))
  }

  def fillNull(df: DataFrame)(colMask: String): DataFrame = {
    df
      .select(
        df.columns.map {
          case x if x.startsWith(colMask) => coalesce(col(x), lit(0)).as(x)
          case y => col(y)
        }: _*
      )
  }

  def joinClientsPivot(clients: Dataset[ClientsCategory],
                       shopPiv: DataFrame,
                       webPiv: DataFrame): DataFrame = {
    clients
      .select(
        col("uid"),
        col("gender"),
        col("age_cat")
      )
      .join(shopPiv, Seq("uid"), "left")
      .join(webPiv, Seq("uid"), "left")
  }
}
