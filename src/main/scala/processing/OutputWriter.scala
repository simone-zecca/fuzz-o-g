package processing

import logging.Loggable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.NullType
import org.apache.spark.sql.{DataFrame, SparkSession}

object OutputWriter extends Loggable {

  def writeOutput(path: String, foundDf: DataFrame, missingDf: DataFrame): Unit = {
    logger.info(s"writing to path:$path")

    //column difference between enriched dataframe and input dataframe
    val columnsToAdd: Array[String] = foundDf.columns.diff( missingDf.columns )

    //add missing columns to input dataframe as empty columns
    columnsToAdd.foldLeft(missingDf) { (df, colname) =>
      df.withColumn(colname, lit(None).cast(NullType))
    }
    missingDf.union(foundDf)
      .sort()
      .repartition(1)
      .write
      .format("csv")
      .option("header", "true")
      .option("delimiter", "|")
      .save(s"/home/npodevkit/zeppelin_0.7.3/share/FUZZ-OGRAPHY/20180822/final_result.csv")
  }

}
