import org.apache.spark.internal.Logging
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, IndexToString, StringIndexer}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import read.Reader
import read.Reader.TrainData
import spark.{SparkLogStatistics, SparkSessionConnector, SparkUtils}
import spark.SparkUtils.{ClearedTrainData, TestFeatures, TrainFeatures}
import write.Writer

object train extends App
  with SparkSessionConnector
  with Logging
  with SparkLogStatistics {

  val hdfsDataPath: String = spark.conf.get("spark.mlproject.data_dir",
    "/labs/laba07/laba07.json")
  val hdfsModelPath: String = spark.conf.get("spark.mlproject.model_dir",
    "/user/sergey.vyun/labs/lab07/model")
  val logUID: String = spark.conf.get("spark.mlproject.log_uid",
    "d50192e5-c44e-4ae8-ae7a-7cfe67c8b777")

  import spark.implicits._

  logInfo(s"[LAB07] Spark version: ${spark.version}")
  logInfo(s"[LAB07] Train Data HDFS path: $hdfsDataPath")
  logInfo(s"[LAB07] Model HDFS path: $hdfsModelPath")
  logInfo(s"[LAB07] Logging UID: $logUID")

  val trainDS: Dataset[TrainData] = Reader.readTrain(hdfsDataPath)
  logInfoStatistics[TrainData](trainDS, "Train Data", logUID)

  val clearedDS: Dataset[ClearedTrainData] = SparkUtils.clearTrain(trainDS)
  logInfoStatistics[ClearedTrainData](clearedDS, "Cleared Train Data", logUID)

  val featuresDS: Dataset[TrainFeatures] = SparkUtils.featuresTrain(clearedDS)
  logInfoStatistics[TrainFeatures](featuresDS, "Features Train Data", logUID)

  val countVectorizer: CountVectorizer = new CountVectorizer()
    .setInputCol("domains")
    .setOutputCol("features")

  val stringIndexer: StringIndexer = new StringIndexer()
    .setInputCol("gender_age")
    .setOutputCol("label")

  val labels: Array[String] = stringIndexer.fit(featuresDS).labels

  val logisticRegression: LogisticRegression = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0.001)

  val indexToString: IndexToString = new IndexToString()
    .setLabels(labels)
    .setInputCol("prediction")
    .setOutputCol("prediction_gender_age")

  val pipeline: Pipeline = new Pipeline()
    .setStages(Array(countVectorizer, stringIndexer, logisticRegression, indexToString))

  val model: PipelineModel = pipeline.fit(featuresDS)

  val testDS: Dataset[TestFeatures] = featuresDS
    .filter(featuresDS("uid") === logUID)
    .select(
      featuresDS("uid"),
      featuresDS("domains")
    ).as[TestFeatures]
  logInfoStatistics[TestFeatures](testDS, "Features Test Data", logUID)

  val testPredict: DataFrame = model.transform(testDS)
  logInfoStatistics[Row](testPredict, "Prediction Test Data", logUID)

  Writer.writeModel(model, hdfsModelPath)
  logInfo(s"[LAB07] Model Saved to path: $hdfsModelPath")
}
