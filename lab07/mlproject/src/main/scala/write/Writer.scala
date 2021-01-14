package write

import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.{Dataset, SaveMode}

object Writer {

  def write[T](data: Dataset[T],
               path: String,
               format: String = "parquet",
               options: Map[String, String] = Map.empty[String, String],
               mode: SaveMode = SaveMode.Overwrite): Unit = {
    data.write
      .format(format)
      .mode(mode)
      .options(options)
      .save(path)
  }

  def writeModel(model: MLWritable, path: String): Unit = {
    model.write
      .overwrite
      .save(path)
  }
}
