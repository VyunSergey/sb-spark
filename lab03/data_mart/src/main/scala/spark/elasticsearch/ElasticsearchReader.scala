package spark.elasticsearch

import org.apache.spark.sql.{Dataset, Encoder, SaveMode, SparkSession}

case class ElasticsearchReader(conf: ElasticsearchConfig) {
  def read[T](index: String)
             (implicit spark: SparkSession, encoder: Encoder[T]): Dataset[T] = {
    spark.read
      .format("org.elasticsearch.spark.sql")
      .option("es.nodes", conf.host)
      .load(index)
      .as[T]
  }

  def write[T](dataset: Dataset[T])
              (index: String)
              (saveMode: SaveMode = SaveMode.Overwrite): Unit = {
    dataset.write
      .format("org.elasticsearch.spark.sql")
      .mode(saveMode)
      .option("es.nodes", conf.host)
      .option("es.port", conf.port)
      .save(index)
  }
}
