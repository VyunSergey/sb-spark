import org.apache.spark.internal.Logging
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions.{col, struct, to_json}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset}
import read.Reader
import read.Reader.TestData
import spark.SparkUtils.{ClearedTestData, TestFeatures}
import spark.{SparkSessionConnector, SparkUtils}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.concurrent.duration.DurationInt
import scala.util.Random

object test extends App
  with SparkSessionConnector
  with Logging {

  val hdfsModelPath: String = spark.conf.get("spark.mlproject.model_dir",
    "/user/sergey.vyun/labs/lab07/model")
  val kafkaTestHosts = spark.conf.get("spark.mlproject.test.kafka.hosts",
    "spark-master-1:6667")
  val kafkaTestStartingOffsets = spark.conf.get("spark.mlproject.test.kafka.starting_offsets",
    "earliest")
  val kafkaTestMaxOffsetsPerTrigger = spark.conf.get("spark.mlproject.test.kafka.max_offsets",
    "1000")
  val kafkaTestInputTopic: String = spark.conf.get("spark.mlproject.test.kafka.input_topic",
    "sergey_vyun")
  val kafkaTestOutputTopic: String = spark.conf.get("spark.mlproject.test.kafka.output_topic",
    "sergey_vyun_lab04b_out")

  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy_MM_dd_hh_mm_ss")
  val dateTimeNow: String = LocalDateTime.now.format(formatter)
  val kafkaCheckPointLocation = spark.conf.get("spark.agg.kafka.checkpoint.location",
    s"/tmp/sergey.vyun/chk/lab04/state_${dateTimeNow}_${Random.nextInt(1000)}")

  logInfo(s"[LAB07] Spark version: ${spark.version}")
  logInfo(s"[LAB07] Model HDFS path: $hdfsModelPath")
  logInfo(s"[LAB07] Test Kafka Hosts: $kafkaTestHosts")
  logInfo(s"[LAB07] Test Input Kafka Topic: $kafkaTestInputTopic")
  logInfo(s"[LAB07] Test Input Kafka StartingOffsets: $kafkaTestStartingOffsets")
  logInfo(s"[LAB07] Test Input Kafka MaxOffsetsPerTrigger: $kafkaTestMaxOffsetsPerTrigger")
  logInfo(s"[LAB07] Test Output Kafka Topic: $kafkaTestOutputTopic")

  val kafkaOptions: Map[String, String] =
    Map(
      "kafka.bootstrap.servers" -> kafkaTestHosts,
      "startingOffsets" -> kafkaTestStartingOffsets,
      "maxOffsetsPerTrigger" -> kafkaTestMaxOffsetsPerTrigger
    )

  val testDS: Dataset[TestData] = Reader.readTestStream(kafkaTestInputTopic, kafkaOptions).distinct

  val clearedDS: Dataset[ClearedTestData] = SparkUtils.clearTest(testDS)

  val featuresDS: Dataset[TestFeatures] = SparkUtils.featuresTest(clearedDS)

  val model: PipelineModel = PipelineModel.load(hdfsModelPath)

  val predict: DataFrame = model.transform(featuresDS)

  val result: DataFrame = predict
    .select(
      to_json(struct(
        col("uid"),
        col("prediction_gender_age").as("gender_age")
      )).as("value")
    )

  result
    .writeStream
    .format("kafka")
    .outputMode(OutputMode.Update)
    .trigger(Trigger.ProcessingTime(5.seconds))
    .option("kafka.bootstrap.servers", kafkaTestHosts)
    .option("topic", kafkaTestOutputTopic)
    .option("checkpointLocation", kafkaCheckPointLocation)
    .start
    .awaitTermination(3.minutes.toMillis)
}
