package spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

trait SparkSessionConnector {

  implicit lazy val spark: SparkSession = SparkSession.builder
    .appName("VyunSergey Lab007")
    .enableHiveSupport
    .config("spark.executor.instances", "10")
    .config("spark.executor.cores", "8")
    .config("spark.executor.memory", "4g")
    .config("spark.default.parallelism", 100)
    .config("spark.sql.shuffle.partitions",100)
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate

  spark.sparkContext.setLogLevel("INFO")
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
}
