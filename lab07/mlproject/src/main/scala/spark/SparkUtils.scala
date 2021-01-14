package spark

import org.apache.spark.sql.{Column, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import read.Reader.{TestData, TrainData}

import scala.reflect.runtime.universe.TypeTag

object SparkUtils {

  case class ClearedTrainData(uid: String, gender_age: String, timestamp: String, domain: String, url: String) {
    override def toString: String =
      s"ClearedTrainData(uid='$uid', gender_age='$gender_age', timestamp='$timestamp', domain='$domain', url='$url')"
  }
  case class TrainFeatures(uid: String, gender_age: String, domains: Array[String]) {
    override def toString: String =
      s"TrainFeatures(uid='$uid', gender_age='$gender_age', domains=${domains.mkString("[", ", ", "]")})"
  }
  case class ClearedTestData(uid: String, timestamp: String, domain: String, url: String) {
    override def toString: String =
      s"ClearedTestData(uid='$uid', timestamp='$timestamp', domain='$domain', url='$url')"
  }
  case class TestFeatures(uid: String, domains: Array[String]) {
    override def toString: String =
      s"TestFeatures(uid='$uid', domains=${domains.mkString("[", ", ", "]")})"
  }

  def clearURL(url: Column): Column = {
    regexp_replace(
      regexp_replace(
        regexp_replace(url,
          "(http(s)?:\\/\\/https(:)?\\/\\/)", "https:\\/\\/"),
        "(http(s)?:\\/\\/http(:)?\\/\\/)", "http:\\/\\/"),
      "www\\.", "")
  }

  def parseDomainJavaURLDecoder(url: Column): Column = {
    lower(trim(SparkUDFs.javaURLDecoderUDF(clearURL(url))))
  }

  def parseDomainSparkURLDecoder(url: Column): Column = {
    lower(trim(callUDF("parse_url", clearURL(url), lit("HOST"))))
  }

  def convertTimestamp(timestamp: Column): Column = {
    from_unixtime(timestamp / 1000)
  }

  def clearData[T1 <: Product : TypeTag,
                T2 <: Product : TypeTag](data: Dataset[T1])
                                        (implicit spark: SparkSession): Dataset[T2] = {
    import spark.implicits._

    data
      .withColumn("visits", explode_outer(col("visits")))
      .withColumn("timestamp", convertTimestamp(col("visits.timestamp")))
      .withColumn("domain", parseDomainJavaURLDecoder(col("visits.url")))
      .withColumn("url", col("visits.url"))
      .drop("visits")
      .as[T2]
  }

  def clearTrain(data: Dataset[TrainData])
                (implicit spark: SparkSession): Dataset[ClearedTrainData] = {
    clearData[TrainData, ClearedTrainData](data)
  }

  def clearTest(data: Dataset[TestData])
               (implicit spark: SparkSession): Dataset[ClearedTestData] = {
    clearData[TestData, ClearedTestData](data)
  }

  def featuresData[T1 <: Product : TypeTag,
                   T2 <: Product : TypeTag](data: Dataset[T1])
                                           (implicit spark: SparkSession): Dataset[T2] = {
    import spark.implicits._

    data
      .groupBy(col("uid"))
      .agg(
        collect_list(col("domain")).as("domains"),
        data.columns
          .filterNot(List("uid", "domain").contains(_))
          .map(nm => max(col(nm)).as(nm)): _*)
      .select(
        col("uid") +:
          data.columns
            .filterNot(List("uid", "domain").contains(_))
            .map(col) :+
          col("domains"): _*)
      .drop("timestamp")
      .as[T2]
  }

  def featuresTrain(data: Dataset[ClearedTrainData])
                   (implicit spark: SparkSession): Dataset[TrainFeatures] = {
    featuresData[ClearedTrainData, TrainFeatures](data)
  }

  def featuresTest(data: Dataset[ClearedTestData])
                  (implicit spark: SparkSession): Dataset[TestFeatures] = {
    featuresData[ClearedTestData, TestFeatures](data)
  }
}
