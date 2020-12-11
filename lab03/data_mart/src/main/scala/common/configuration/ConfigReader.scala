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
      srcCassandraHost = parseLines(resource, "src-cassandra-host").get,
      srcCassandraPort = parseLines(resource, "src-cassandra-port").get,
      srcCassandraKeySpace = parseLines(resource, "src-cassandra-key-space").get,
      srcCassandraTable = parseLines(resource, "src-cassandra-table").get,
      srcElasticsearchHost = parseLines(resource, "src-elasticsearch-host").get,
      srcElasticsearchIndex = parseLines(resource, "src-elasticsearch-index").get,
      srcHDFSPath = parseLines(resource, "src-hdfs-path").get,
      srcPostgresHost = parseLines(resource, "src-postgres-host").get,
      srcPostgresPort = parseLines(resource, "src-postgres-port").get,
      srcPostgresSchema = parseLines(resource, "src-postgres-schema").get,
      srcPostgresTable = parseLines(resource, "src-postgres-table").get,
      tgtPostgresHost = parseLines(resource, "tgt-postgres-host").get,
      tgtPostgresPort = parseLines(resource, "tgt-postgres-port").get,
      tgtPostgresSchema = parseLines(resource, "tgt-postgres-schema").get,
      tgtPostgresTable = parseLines(resource, "tgt-postgres-table").get
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
    value.trim.toLowerCase.replaceAll("\"", "")
  }
}

object ConfigReader extends ConfigReader {

  final case class Config(
                           srcCassandraHost: String,
                           srcCassandraPort: String,
                           srcCassandraKeySpace: String,
                           srcCassandraTable: String,
                           srcElasticsearchHost: String,
                           srcElasticsearchIndex: String,
                           srcHDFSPath: String,
                           srcPostgresHost: String,
                           srcPostgresPort: String,
                           srcPostgresSchema: String,
                           srcPostgresTable: String,
                           tgtPostgresHost: String,
                           tgtPostgresPort: String,
                           tgtPostgresSchema: String,
                           tgtPostgresTable: String
                         )
}
