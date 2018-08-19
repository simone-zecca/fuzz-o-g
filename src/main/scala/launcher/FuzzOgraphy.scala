package launcher

import java.time.Instant

import configuration.{Configuration, ConfigurationReader}
import logging.{Loggable, LoggerLoader}
import org.apache.spark.sql.{DataFrame, SparkSession}
import processing.{GeoReader, InputReader, Matcher}

object FuzzOgraphy {
  def apply(settingsPath: String)(implicit spark: SparkSession): FuzzOgraphy = {
    new FuzzOgraphy(settingsPath)(spark)
  }
}

class FuzzOgraphy(settingsPath: String)(implicit val spark: SparkSession) extends Loggable {

  val startTime: String = Instant.now().toString

  logger.info(s"settingsPath:$settingsPath")
  val configuration: Configuration = ConfigurationReader.readFromPath(settingsPath)

  val logFile: String =
    configuration.processing.logfilePrefix + startTime +
      configuration.processing.logfileSuffix

  LoggerLoader.loadConfigurationFromFile(
    configurationFile = configuration.processing.loggingProperties,
    loggerOutputPath = logFile
  )

  val printSample: Boolean = configuration.processing.showDataframeSample;

  def run(): Unit = {
    logger.info("process Started")

    val inputCitiesDF = InputReader.readCities(
      configuration.inputFiles.basePath + configuration.inputFiles.inputCities
    ).transform(InputReader.cleanCities)
    traceDf("inputCitiesDF", inputCitiesDF, printSample)

    val geoCitiesDF = GeoReader.readGeoCities(
      configuration.geoData.basePath + configuration.geoData.geoCities
    ).transform(GeoReader.cleanGeoCities)
    traceDf("geoCitiesDF", geoCitiesDF, printSample)

    var outputDf = inputCitiesDF
      .transform(Matcher.perfectMatch(geoCitiesDF)).persist()
      .transform(Matcher.getMatched)
      .transform(Matcher.checkDuplicates)
    printDf("outputDf containing perfect Matches", outputDf)

    var missingDf = inputCitiesDF
      .transform(Matcher.perfectMatch(geoCitiesDF)).persist()
      .transform(Matcher.getMissingInputCities)
    printDf("missingDf after perfect Matches", missingDf)

    var outputDfDist = missingDf
      .transform(Matcher.distanceMatch(geoCitiesDF, 20)).persist()
      .transform(Matcher.getMatched)
      .transform(Matcher.checkDuplicates)
    printDf("outputDfDist containing 20% distance matches", missingDf)

    missingDf = missingDf
      .transform(Matcher.distanceMatch(geoCitiesDF, 20)).persist()
      .transform(Matcher.getMissingInputCities)
    printDf("missingDf after 20% distance matches", missingDf)

  }

  def traceDf(dataFrameName: String, dataframe: DataFrame, showDataframeContent: Boolean): Unit = {
    if (showDataframeContent) {
      //this is really costly and should be used only in debugging phase
      dataframe.show(10, false)
      println(s"dataFrame:$dataFrameName")
      println("=======================================")
    }
  }

  def printDf(dataFrameName: String, dataframe: DataFrame): Unit = {
    //TODO:Remove
    dataframe.persist()
    val lines : Int = dataframe.count().toInt;
    dataframe.show(lines, false)
    println(s"dataFrame:$dataFrameName")
    println(s"lines:$lines")
    println("=======================================")
  }

}
