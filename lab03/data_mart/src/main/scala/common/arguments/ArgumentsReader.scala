package common.arguments

import common.arguments.ArgumentsReader._

import scala.annotation.tailrec

class ArgumentsReader(args: Seq[String]) {
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

  lazy val arguments: Arguments = Arguments(
    cassandraKeySpace = parseArgs(args, "--cassandra-key-space"),
    cassandraTable = parseArgs(args, "--cassandra-table"),
    elasticsearchIndex = parseArgs(args, "--elasticsearch-index"),
    hdfsPath = parseArgs(args, "--hdfs-path"),
    postgresUser = parseArgs(args, "--postgres-user"),
    postgresPassword = parseArgs(args, "--postgres-password"),
    postgresSrcSchema = parseArgs(args, "--postgres-src-schema"),
    postgresSrcTable = parseArgs(args, "--postgres-src-table"),
    postgresTgtSchema = parseArgs(args, "--postgres-tgt-schema"),
    postgresTgtTable = parseArgs(args, "--postgres-tgt-table")
  )
}

object ArgumentsReader {
  def apply(args: Seq[String]): ArgumentsReader = new ArgumentsReader(args)

  case class Arguments(
                        cassandraKeySpace: Option[String],
                        cassandraTable: Option[String],
                        elasticsearchIndex: Option[String],
                        hdfsPath: Option[String],
                        postgresUser: Option[String],
                        postgresPassword: Option[String],
                        postgresSrcSchema: Option[String],
                        postgresSrcTable: Option[String],
                        postgresTgtSchema: Option[String],
                        postgresTgtTable: Option[String]
                      )
}
