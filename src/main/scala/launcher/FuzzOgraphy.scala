package launcher

import java.time.Instant

import configuration.{Configuration, ConfigurationReader}
import logging.{Loggable, LoggerLoader}
import org.apache.spark.sql.{DataFrame, SparkSession}
import processing.{GeoReader, InputReader, Matcher, OutputWriter}

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

  val printSample: Boolean = configuration.processing.showDataframeSample

  def run(): Unit = {
    logger.info("process Started")

    val inputCitiesDF = InputReader.readCities(
      configuration.inputFiles.basePath + configuration.inputFiles.inputCities
    ).transform(InputReader.cleanCities)
    //traceDf("inputCitiesDF", inputCitiesDF, printSample)

    val geoCitiesDF = GeoReader.readGeoCities(
      configuration.geoData.basePath + configuration.geoData.geoCities
    ).transform(GeoReader.cleanGeoCities)
    //traceDf("geoCitiesDF", geoCitiesDF, printSample)

    val leftDf = inputCitiesDF.select("Input_CountryCode", "normalized_city").distinct()
    traceDf("leftDf at begin", leftDf, printSample)

    val rightDf = geoCitiesDF
      .select("Output_CountryCode", "normalized_geo_city")
     .distinct()
    traceDf("rightDf at begin", rightDf, printSample)


    var outputDf = leftDf
      .transform(Matcher.perfectMatch(rightDf)).persist()
      .transform(Matcher.getMatched)
      .transform(Matcher.checkDuplicates)
    traceDf("outputDf containing perfect Matches", outputDf, printSample)

    var missingDf = leftDf
      .transform(Matcher.getMissingInputCities(outputDf))//.persist()
    traceDf("missingDf after perfect Matches", missingDf, printSample)

    //the processing will be performed one time for each distance
    //TODO:move in configuration hocon
    val distances = List(10,15,20,25,30,35,40,45)

    for (distance <- distances) {
      logger.info(s"matching on a distance of:$distance")

      val outputDfTmp = missingDf
        .transform(Matcher.distanceMatch(rightDf, distance)).persist()
        .transform(Matcher.getMatched)
        .transform(Matcher.checkDuplicates)
      traceDf(s"outputDfTmp containing $distance% distance matches", outputDfTmp, printSample)

      missingDf = missingDf
        .transform(Matcher.getMissingInputCities(outputDfTmp))//.persist()
      traceDf(s"missingDfTmp after $distance% distance matches", missingDf, printSample)

      outputDf = outputDf.union(outputDfTmp)

//      outputDf
//        .repartition(1)
//        .sort()
//        .write
//        .format("csv")
//        .option("header", "true")
//        .option("delimiter", "|")
//        .save(s"/home/npodevkit/FUZZ-OGRAPHY/test/${distance}_output.csv")

//      missingDf
//        .repartition(1)
//        .sort()
//        .write
//        .format("csv")
//        .option("header", "true")
//        .option("delimiter", "|")
//        .save(s"/home/npodevkit/FUZZ-OGRAPHY/test/${distance}_missing.csv")

    }

    OutputWriter.writeOutput(configuration.output.path, inputCitiesDF, outputDf)
  }

  def traceDf(dataFrameName: String, dataframe: DataFrame, showDataframeContent: Boolean): Unit = {
    //this is costly and to be used only in debugging
    if (showDataframeContent) {
      dataframe.persist()
      val lines : Int = dataframe.count().toInt
      dataframe.show(10, truncate = false)
      println(s"dataFrame:$dataFrameName")
      println(s"lines:$lines")
      println("=======================================")
    }
  }

}
