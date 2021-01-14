package spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Dataset

trait SparkLogStatistics {
  this: Logging =>

  def logInfoStatistics[T](ds: Dataset[T],
                           name: String,
                           uid: String = "",
                           logFunction: String => Unit = str => logInfo(str)): Unit = {
    logFunction(s"[LAB07] $name count: ${ds.count}")
    logFunction(s"[LAB07] $name schema:\n${ds.schema.treeString}")
    logFunction(s"[LAB07] $name sample:\n${ds.take(10).mkString("\n")}\n")
    logFunction(s"[LAB07] $name sample uid = '$uid':\n${takeByUid(ds, uid)}\n")
  }

  def takeByUid[T](ds: Dataset[T], uid: String, rows: Int = 100): String = {
    if (ds.columns.map(_.trim.toLowerCase).contains("uid"))
      ds.filter(ds("uid") === uid).take(rows).mkString("\n")
    else ""
  }
}
