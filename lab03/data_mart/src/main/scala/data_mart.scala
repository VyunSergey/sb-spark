import common.arguments.ArgumentsReader
import common.configuration.ConfigReader
import domain.{Clients, DomainCategories, OnlineLogs, WebLogs}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, SparkSession}
import spark.SparkReader
import spark.cassandra.{CassandraConfig, CassandraReader}
import spark.elasticsearch.{ElasticsearchConfig, ElasticsearchReader}
import spark.postgresql.{PostgreSQLConfig, PostgreSQLReader}

object data_mart extends App with Logging {
  // Arguments
  val arguments: ArgumentsReader.Arguments = ArgumentsReader(args).arguments

  // Config
  val configName: String = "/lab03/application.conf"
  val config: ConfigReader.Config = ConfigReader.readResource(configName)

  // Cassandra
  val cassandraConfig: CassandraConfig = CassandraConfig(config.cassandraHost, config.cassandraPort)
  val cassandraKeySpace: String = arguments.cassandraKeySpace.getOrElse(config.cassandraKeySpace)
  val cassandraTable: String = arguments.cassandraTable.getOrElse(config.cassandraTable)

  // Elasticsearch
  val elasticsearchConfig: ElasticsearchConfig = ElasticsearchConfig(config.elasticsearchHost, config.elasticsearchPort)
  val elasticsearchIndex: String = arguments.elasticsearchIndex.getOrElse(config.elasticsearchIndex)

  // HDFS
  val hdfsPath: String = arguments.hdfsPath.getOrElse(config.hdfsPath)

  // PostgreSQL
  val postgreSQLConfig: PostgreSQLConfig =
    PostgreSQLConfig(
      host = config.postgresHost,
      port = config.postgresPort,
      user = arguments.postgresUser.getOrElse(config.postgresUser),
      password = arguments.postgresPassword.getOrElse(config.postgresPassword)
    )
  val postgresSrcSchema: String = arguments.postgresSrcSchema.getOrElse(config.postgresSrcSchema)
  val postgresSrcTable: String = arguments.postgresSrcTable.getOrElse(config.postgresSrcTable)

  val postgresTgtSchema: String = arguments.postgresTgtSchema.getOrElse(config.postgresTgtSchema)
  val postgresTgtTable: String = arguments.postgresTgtTable.getOrElse(config.postgresTgtTable)

  // Spark
  implicit lazy val spark: SparkSession = SparkSession.builder()
    .appName("Vyun Sergey Lab03")
    .getOrCreate()

  spark.sparkContext.setLogLevel("INFO")

  import spark.implicits._

  logInfo(s"[Lab03] Config: $config")

  val cassandra: CassandraReader = CassandraReader(cassandraConfig)
  val clients: Dataset[Clients] = cassandra.read(cassandraKeySpace, cassandraTable)
  // [Lab03] Clients count: 36.138
  logInfo(s"[Lab03] Clients count: ${clients.count}")
  logInfo(s"[Lab03] Clients schema:\n${clients.printSchema}")
  logInfo(s"[Lab03] Clients sample:\n${clients.take(10).mkString("\n")}")

  val elasticsearch: ElasticsearchReader = ElasticsearchReader(elasticsearchConfig)
  val onlineLogs: Dataset[OnlineLogs] = elasticsearch.read(elasticsearchIndex)
  // [Lab03] OnlineLogs count: 182.540
  logInfo(s"[Lab03] OnlineLogs count: ${onlineLogs.count}")
  logInfo(s"[Lab03] OnlineLogs schema:\n${onlineLogs.printSchema}")
  logInfo(s"[Lab03] OnlineLogs sample:\n${onlineLogs.take(10).mkString("\n")}")

  val postgresql = PostgreSQLReader(postgreSQLConfig)
  val domainCategories: Dataset[DomainCategories] = postgresql.read(postgresSrcSchema, postgresSrcTable)
  // [Lab03] DomainCategories count: 245.981
  logInfo(s"[Lab03] DomainCategories count: ${domainCategories.count}")
  logInfo(s"[Lab03] DomainCategories schema:\n${domainCategories.printSchema}")
  logInfo(s"[Lab03] DomainCategories sample:\n${domainCategories.take(10).mkString("\n")}")

  val webLogs: Dataset[WebLogs] = SparkReader.json(hdfsPath)
  // [Lab03] WebLogs count: 36.138
  logInfo(s"[Lab03] WebLogs count: ${webLogs.count}")
  logInfo(s"[Lab03] WebLogs schema:\n${webLogs.printSchema}")
  logInfo(s"[Lab03] WebLogs sample:\n${webLogs.take(10).mkString("\n")}")
}
