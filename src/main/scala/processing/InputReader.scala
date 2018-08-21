package processing

import logging.Loggable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import spark.Functions._

object InputReader extends Loggable {

  def readCities(path: String)(implicit spark: SparkSession): DataFrame = {
    logger.info(s"reading from path:$path")
    spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      .option("quote", "\'")
      .load(path)
  }

  def cleanCities(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .withColumnRenamed("CountryCode", "Input_CountryCode")
      .withColumnRenamed("city", "Input_City")
      .withColumn("normalized_city", concat(clean_city(col("Input_City"))))
      .distinct()
  }

}
