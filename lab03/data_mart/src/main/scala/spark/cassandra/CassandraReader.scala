package spark.cassandra

import org.apache.spark.sql.{Dataset, Encoder, SaveMode, SparkSession}

case class CassandraReader(conf: CassandraConfig) {
  def withConfig[A](f: => A)(implicit spark: SparkSession): A = {
    spark.conf.set("spark.cassandra.connection.host", conf.host)
    spark.conf.set("spark.cassandra.connection.port", conf.port)
    spark.conf.set("spark.cassandra.output.consistency.level", "ANY")
    spark.conf.set("spark.cassandra.input.consistency.level", "ONE")
    f
  }

  def read[T](keyspace: String, table: String)
             (implicit spark: SparkSession, encoder: Encoder[T]): Dataset[T] = {
    withConfig {
      spark.read
        .format("org.apache.spark.sql.cassandra")
        .option("keyspace", keyspace)
        .option("table", table)
        .load()
        .as[T]
    }
  }

  def write[T](dataset: Dataset[T])
              (keyspace: String, table: String)
              (saveMode: SaveMode = SaveMode.Overwrite)
              (implicit spark: SparkSession): Unit = {
    withConfig {
      dataset.write
        .format("org.apache.spark.sql.cassandra")
        .mode(saveMode)
        .option("keyspace", keyspace)
        .option("table", table)
        .save()
    }
  }
}
