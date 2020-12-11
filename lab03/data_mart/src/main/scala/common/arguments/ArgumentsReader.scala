package common.arguments

import common.arguments.ArgumentsReader._

import scala.annotation.tailrec

class ArgumentsReader(args: Seq[String]) {
  @tailrec
  final def parseArgs(args: Seq[String], key: String): Option[String] = args match {
    case k :: v :: _ if parseKey(key)(k) => Some(v)
    case k :: v :: Nil if parseKey(key)(k) => Some(v)
    case k :: xs if !parseKey(key)(k) => parseArgs(xs, key)
    case k :: Nil if !parseKey(key)(k) => None
    case Nil => None
  }

  final def parseKey(key: String)(arg: String): Boolean = {
    key.trim.toLowerCase == arg.trim.toLowerCase
  }

  lazy val arguments: Arguments = Arguments(
    srcCassandraKeySpace = parseArgs(args, "--src-cassandra-key-space"),
    srcCassandraTable = parseArgs(args, "--src-cassandra-table"),
    srcElasticsearchIndex = parseArgs(args, "--src-elasticsearch-index"),
    srcHDFSPath = parseArgs(args, "--src-hdfs-path"),
    srcPostgresSchema = parseArgs(args, "--src-postgres-schema"),
    srcPostgresTable = parseArgs(args, "--src-postgres-table"),
    tgtPostgresSchema = parseArgs(args, "--tgt-postgres-schema"),
    tgtPostgresTable = parseArgs(args, "--tgt-postgres-table")
  )
}

object ArgumentsReader {
  def apply(args: Seq[String]): ArgumentsReader = new ArgumentsReader(args)

  case class Arguments(
                        srcCassandraKeySpace: Option[String],
                        srcCassandraTable: Option[String],
                        srcElasticsearchIndex: Option[String],
                        srcHDFSPath: Option[String],
                        srcPostgresSchema: Option[String],
                        srcPostgresTable: Option[String],
                        tgtPostgresSchema: Option[String],
                        tgtPostgresTable: Option[String]
                      )
}
