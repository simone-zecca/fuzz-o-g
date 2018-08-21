package processing

import logging.Loggable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import spark.Functions._

object Matcher extends Loggable {

  def perfectMatch(geoCitiesDf: DataFrame)(citiesDf: DataFrame): DataFrame = {
    citiesDf
      .join(
        geoCitiesDf,
        citiesDf("Input_CountryCode") === geoCitiesDf("Output_CountryCode") &&
        citiesDf("normalized_city") === geoCitiesDf("normalized_geo_city"),
        "left_outer"
      )
  }

  def distanceMatch(geoCitiesDf: DataFrame, distancePerCent: Int)(citiesDf: DataFrame): DataFrame = {
    citiesDf
      .join(
        geoCitiesDf,
          citiesDf("Input_CountryCode") === geoCitiesDf("Output_CountryCode") &&
          //( levenshtein(citiesDf("normalized_city"), geoCitiesDf("normalized_geo_city")) <= 1 ),

          /*  Percentage of word distance between city and geo-city.
              Formula:
              1/(city_lenght/(levenshtein_distance*100))  */
        ( lit(1)
          / ( length(citiesDf("normalized_city"))
            / (levenshtein(citiesDf("normalized_city"), geoCitiesDf("normalized_geo_city")) * 100) ) <= distancePerCent),
        "left_outer")
      .dropDuplicates("normalized_city")
  }

  def getMatched(joinedDf: DataFrame): DataFrame = {
    joinedDf.filter(col("normalized_geo_city").isNotNull)
  }

  def getMissingInputCities(foundDf: DataFrame)(sourceDf: DataFrame): DataFrame = {
    //left anti-join to remove already found cities
    //foundDf.printSchema()
    //sourceDf.printSchema()

    //Look at https://issues.apache.org/jira/browse/SPARK-14948
    val renamedDf =
    foundDf
      .withColumnRenamed("Input_CountryCode", "Input_CountryCode1")
      .withColumnRenamed("normalized_city", "normalized_city1")

    sourceDf.join(
      renamedDf,
      sourceDf("Input_CountryCode") === renamedDf("Input_CountryCode1") &&
      sourceDf("normalized_city") === renamedDf("normalized_city1"),
      "left_anti"
    )
  }

  def checkDuplicates(joinedDf: DataFrame): DataFrame = {
    val duplicatesDf =
      joinedDf
        .groupBy( "Input_CountryCode", "normalized_city" )
        .agg(count("*").as("matching"))
        .where(col("matching") > 1)

    val duplicatesCount: Long = duplicatesDf.count()
    if ( duplicatesCount > 0) {
      val message = s"************************** ${duplicatesCount} Duplicates Found!"
      logger.error(message)
      duplicatesDf
        .sort( "Input_CountryCode", "normalized_city")
        .show(duplicatesCount.toInt, false)
      throw new RuntimeException(message)
    }
    joinedDf
  }

}
