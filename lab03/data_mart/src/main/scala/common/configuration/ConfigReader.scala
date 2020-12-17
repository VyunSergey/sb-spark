package common.configuration

import java.io.InputStream

import common.configuration.ConfigReader._

import scala.annotation.tailrec
import scala.io.Source

trait ConfigReader {
  def readResource(name: String): Config = {
    val resourceStream: InputStream = this.getClass.getResourceAsStream(name)
    val resource: List[String] = Source.fromInputStream(resourceStream).getLines.toList

    Config(
      cassandraHost = parseLines(resource, "cassandra-host").get,
      cassandraPort = parseLines(resource, "cassandra-port").get,
      cassandraKeySpace = parseLines(resource, "cassandra-key-space").get,
      cassandraTable = parseLines(resource, "cassandra-table").get,
      elasticsearchHost = parseLines(resource, "elasticsearch-host").get,
      elasticsearchPort = parseLines(resource, "elasticsearch-port").get,
      elasticsearchIndex = parseLines(resource, "elasticsearch-index").get,
      hdfsPath = parseLines(resource, "hdfs-path").get,
      postgresHost = parseLines(resource, "postgres-host").get,
      postgresPort = parseLines(resource, "postgres-port").get,
      postgresUser = parseLines(resource, "postgres-user").get,
      postgresPassword = parseLines(resource, "postgres-password").get,
      postgresSrcSchema = parseLines(resource, "postgres-src-schema").get,
      postgresSrcTable = parseLines(resource, "postgres-src-table").get,
      postgresTgtSchema = parseLines(resource, "postgres-tgt-schema").get,
      postgresTgtTable = parseLines(resource, "postgres-tgt-table").get
    )
  }

  final def parseLines(lines: Seq[String], key: String): Option[String] = {
    lines.map(parseLine(key)).reduce(_ orElse _)
  }

  final def parseLine(key: String)(line: String): Option[String] = {
    val args = line.split("([ =])").filter(_.nonEmpty).toList
    parseArgs(args, key)
  }

  @tailrec
  final def parseArgs(args: Seq[String], key: String): Option[String] = args match {
    case k :: v :: _ if parseKey(key)(k) => Some(parseValue(v))
    case k :: v :: Nil if parseKey(key)(k) => Some(parseValue(v))
    case k :: xs if !parseKey(key)(k) => parseArgs(xs, key)
    case k :: Nil if !parseKey(key)(k) => None
    case Nil => None
  }

  final def parseKey(key: String)(arg: String): Boolean = {
    key.trim.toLowerCase == arg.trim.toLowerCase
  }

  final def parseValue(value: String): String = {
    value.trim.replaceAll("\"", "")
  }
}

object ConfigReader extends ConfigReader {

  final case class Config(
                           cassandraHost: String,
                           cassandraPort: String,
                           cassandraKeySpace: String,
                           cassandraTable: String,
                           elasticsearchHost: String,
                           elasticsearchPort: String,
                           elasticsearchIndex: String,
                           hdfsPath: String,
                           postgresHost: String,
                           postgresPort: String,
                           postgresUser: String,
                           postgresPassword: String,
                           postgresSrcSchema: String,
                           postgresSrcTable: String,
                           postgresTgtSchema: String,
                           postgresTgtTable: String
                         )
}
