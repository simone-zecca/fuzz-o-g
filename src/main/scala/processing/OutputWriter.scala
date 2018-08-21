package processing

import logging.Loggable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.NullType
import org.apache.spark.sql.{DataFrame, SparkSession}

object OutputWriter extends Loggable {

  def writeOutput(
    path: String,
    inputCitiesDF: DataFrame,
    foundDf: DataFrame): Unit = {

    logger.info(s"writing to path:$path")

    //    //column difference between enriched dataframe and input dataframe
    //    val columnsToAdd: Array[String] = foundDf.columns.diff( missingDf.columns )
    //
    //    //add missing columns to input dataframe as empty columns
    //    columnsToAdd.foldLeft(missingDf) { (df, colname) =>
    //      df.withColumn(colname, lit(None).cast(NullType))
    //    }

    logger.info(s"inputCitiesDF:")
    inputCitiesDF.printSchema()
    logger.info(s"foundDf:")
    foundDf.printSchema()

    val matchedDf = foundDf
      .withColumnRenamed("normalized_city", "normalized_city1")
      .withColumnRenamed("Input_CountryCode", "Input_CountryCode1")
      .withColumnRenamed("normalized_geo_city", "Output_City")

    val outputDf = inputCitiesDF
      .join(
        matchedDf,
        inputCitiesDF("normalized_city") === matchedDf("normalized_city1") &&
          inputCitiesDF("Input_CountryCode") === matchedDf("Input_CountryCode1"),
        "left_outer")

      .drop("normalized_city")
      .drop("Input_CountryCode1")
      .drop("normalized_city1")

    outputDf
      .select("Input_CountryCode", "Input_City", "Output_CountryCode", "Output_City")
      .sort()
      .repartition(1)
      .write
      .format("csv")
      .option("header", "true")
      .option("delimiter", "|")
      .save(s"/home/npodevkit/zeppelin_0.7.3/share/FUZZ-OGRAPHY/20180822/final_result.csv")
  }

}
