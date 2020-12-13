package spark

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

object SparkReader {
  def json[T](path: String, inferSchema: Boolean = false)
             (implicit spark: SparkSession, encoder: Encoder[T]): Dataset[T] = {
    spark.read
      .option("inferSchema", inferSchema)
      .json(path)
      .as[T]
  }
}
