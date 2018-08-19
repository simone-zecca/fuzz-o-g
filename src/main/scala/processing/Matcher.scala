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
        citiesDf("CountryCode") <=> geoCitiesDf("CC_FIPS")
          && citiesDf("normalized_city") === geoCitiesDf("normalized_geo_city"),
        "left_outer"
      )
  }

  def distanceMatch(geoCitiesDf: DataFrame, distancePerCent: Int)(citiesDf: DataFrame): DataFrame = {
    citiesDf
      .join(
        geoCitiesDf,
        citiesDf("CountryCode") <=> geoCitiesDf("CC_FIPS") &&
          //1/(word_lenght/(levenshtein_distance*100))
          //( levenshtein(citiesDf("normalized_city"), geoCitiesDf("normalized_geo_city")) <= 1 ),
          ( lit(1) /
            (length(citiesDf("normalized_city"))
              / (levenshtein(citiesDf("normalized_city"), geoCitiesDf("normalized_geo_city")) * 100)
              )
            <= distancePerCent),
        "left_outer")
      .dropDuplicates("CountryCode","normalized_city")
  }

  def getMatched(joinedDf: DataFrame): DataFrame = {
    joinedDf.filter(col("normalized_geo_city").isNotNull)
  }

  def getMissingInputCities(foundDf: DataFrame)(sourceDf: DataFrame): DataFrame = {
    sourceDf
      .filter(col("normalized_geo_city").isNull)
      .select("CountryCode", "normalized_city")
  }

  def checkDuplicates(joinedDf: DataFrame): DataFrame = {
    val duplicatesDf =
      joinedDf
        .groupBy( "CountryCode","normalized_city" )
        .agg(count("*").as("matching"))
        .where(col("matching") > 1)

    val duplicatesCount: Long = duplicatesDf.count()
    if ( duplicatesCount > 0) {
      val message = s"************************** ${duplicatesCount} Duplicates Found!"
      logger.error(message)
      duplicatesDf
        .sort(col("CountryCode"), col("normalized_city"))
        .show(duplicatesCount.toInt)
      throw new RuntimeException(message)
    }
    joinedDf
  }

}
