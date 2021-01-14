package read

import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types._

import scala.reflect.runtime.universe.TypeTag

object Reader {

  case class Visit(timestamp: Long, url: String) {
    override def toString: String =
      s"Visit(timestamp='$timestamp', url='$url')"
  }
  case class TrainData(uid: String, gender_age: String, visits: Array[Visit]) {
    override def toString: String =
      s"TrainData(uid='$uid', gender_age='$gender_age', visits=${visits.mkString("[", ", ", "]")})"
  }
  case class TestData(uid: String, visits: Array[Visit]) {
    override def toString: String =
      s"TestData(uid='$uid', visits=${visits.mkString("[", ", ", "]")}))"
  }

  val trainSchema: StructType =
    StructType(
      StructField("uid", StringType) ::
        StructField("gender_age", StringType) ::
        StructField("visits", ArrayType(StructType(
          StructField("url", StringType) ::
            StructField("timestamp", LongType) :: Nil
        ))) :: Nil
    )

  val testSchema: StructType =
    StructType(
      StructField("uid", StringType) ::
        StructField("visits", ArrayType(StructType(
          StructField("url", StringType) ::
            StructField("timestamp", LongType) :: Nil
        ))) :: Nil
    )

  def read[T <: Product : TypeTag](path: String,
                                   schema: StructType,
                                   options: Map[String, String] = Map.empty[String, String])
                                  (implicit spark: SparkSession): Dataset[T] = {
    import spark.implicits._

    spark.read
      .format("json")
      .schema(schema)
      .option("inferSchema", "false")
      .options(options)
      .load(path)
      .as[T]
  }

  def readKafkaStream[T <: Product : TypeTag](topic: String,
                                              schema: StructType,
                                              options: Map[String, String] = Map.empty[String, String])
                                             (implicit spark: SparkSession): Dataset[T] = {
    import spark.implicits._

    spark.readStream
      .format("kafka")
      .options(options ++ Map("subscribe" -> topic))
      .load
      .select(
        from_json(col("value").cast(StringType), schema).as("json")
      )
      .select(
        schema.fields.map { field =>
          col(s"json.${field.name}").as(field.name)
        }: _*
      )
      .as[T]
  }

  def readTrain(path: String,
                options: Map[String, String] = Map.empty[String, String])
               (implicit spark: SparkSession): Dataset[TrainData] = {
    read[TrainData](path, trainSchema, options)
  }

  def readTest(path: String,
               options: Map[String, String] = Map.empty[String, String])
              (implicit spark: SparkSession): Dataset[TestData] = {
    read[TestData](path, testSchema, options)
  }

  def readTestStream(topic: String,
                     options: Map[String, String] = Map.empty[String, String])
                    (implicit spark: SparkSession): Dataset[TestData] = {
    readKafkaStream(topic, testSchema, options)
  }
}
