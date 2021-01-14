package spark

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.util.Try

object SparkUDFs {
  val javaURLDecoder: String => Option[String] = (url: String) =>
    Try(new java.net.URL(java.net.URLDecoder.decode(url, "UTF-8")).getHost).toOption

  val javaURLDecoderUDF: UserDefinedFunction = udf(javaURLDecoder)
}
