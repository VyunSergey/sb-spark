package spark

import domain.{Clients, ClientsCategory, DomainCategories, DomainWebCategory, OnlineLogs, ShopCategory}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

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

  def makeWebCategory(ds: Dataset[DomainCategories])
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
}
