import common.arguments.ArgumentsReader
import common.configuration.ConfigReader
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object data_mart extends App with Logging {
  lazy val arguments: ArgumentsReader.Arguments = ArgumentsReader(args).arguments

  lazy val configName: String = "/lab03/application.conf"
  lazy val config: ConfigReader.Config = ConfigReader.readResource(configName)

  lazy val srcCassandraKeySpace: String = arguments.srcCassandraKeySpace.getOrElse(config.srcCassandraKeySpace)
  lazy val srcCassandraTable: String = arguments.srcCassandraTable.getOrElse(config.srcCassandraTable)

  lazy val srcElasticsearchIndex: String = arguments.srcElasticsearchIndex.getOrElse(config.srcElasticsearchIndex)

  lazy val srcHDFSPath: String = arguments.srcHDFSPath.getOrElse(config.srcHDFSPath)

  lazy val srcPostgresSchema: String = arguments.srcPostgresSchema.getOrElse(config.srcPostgresSchema)
  lazy val srcPostgresTable: String = arguments.srcPostgresTable.getOrElse(config.srcPostgresTable)

  lazy val tgtPostgresSchema: String = arguments.tgtPostgresSchema.getOrElse(config.tgtPostgresSchema)
  lazy val tgtPostgresTable: String = arguments.tgtPostgresTable.getOrElse(config.tgtPostgresTable)

  implicit lazy val spark: SparkSession = SparkSession.builder()
    .appName("Vyun Sergey Lab03")
    .getOrCreate()

  logInfo(s"[Lab03] Hello World!")

}
