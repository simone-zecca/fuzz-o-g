package processing

import logging.Loggable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OutputWriter extends Loggable {

  def writeOutput(
    path: String,
    inputCitiesDF: DataFrame,
    foundDf: DataFrame): Unit = {

    logger.info(s"writing to path:$path")

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
      .withColumn("Output_BlankCity", when(col("Output_City").isNotNull && !trim(col("Output_City")).equalTo(""), "No").otherwise("Yes") )
      .select(
        "Input_CountryCode",
        "Input_City",
        "Output_CountryCode",
        "Output_City",
        "Output_BlankCity",
        "distance_percent"
      )
      .sort(
        col("distance_percent").asc_nulls_last,
        col("Output_CountryCode").asc_nulls_last,
        col("Output_City").asc_nulls_last
      )
      .repartition(1)
      .write
      .format("csv")
      .option("header", "true")
      .option("delimiter", "|")
      .save(path)
  }

}
