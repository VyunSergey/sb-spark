package spark.postgresql

import org.apache.spark.sql.{Dataset, Encoder, SaveMode, SparkSession}

case class PostgreSQLReader(conf: PostgreSQLConfig) {
  def read[T](schema: String, table: String)
             (implicit spark: SparkSession, encoder: Encoder[T]): Dataset[T] = {
    spark.read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://${conf.host}:${conf.port}/$schema")
      .option("dbtable", table)
      .option("user", conf.user)
      .option("password", conf.password)
      .option("driver", "org.postgresql.Driver")
      .load()
      .as[T]
  }

  def write[T](dataset: Dataset[T])
              (schema: String, table: String)
              (saveMode: SaveMode = SaveMode.Overwrite): Unit = {
    dataset.write
      .format("jdbc")
      .mode(saveMode)
      .option("url", s"jdbc:postgresql://${conf.host}:${conf.port}/$schema")
      .option("dbtable", table)
      .option("user", conf.user)
      .option("password", conf.password)
      .option("driver", "org.postgresql.Driver")
      .save()
  }
}
