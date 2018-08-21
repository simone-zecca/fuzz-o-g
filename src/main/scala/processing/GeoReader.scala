package processing

import logging.Loggable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import spark.Functions._

object GeoReader extends Loggable {

  def readGeoCities(path: String)(implicit spark: SparkSession): DataFrame = {
    logger.info(s"reading from path:$path")
    spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "\t")
      .option("quote", "\"")
      .load(path)
  }

  def cleanGeoCities(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .withColumnRenamed("FULL_NAME_ND","Output_City")
      .withColumnRenamed("CC_FIPS","Output_CountryCode")
      .withColumn("normalized_geo_city", concat(clean_city(col("Output_City"))))
      .distinct()
  }



}
