package unit.processing

import configuration.{Configuration, ConfigurationReader, GeoData, InputFiles}
import logging.{Loggable, TestLoggerInitializer}
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec
import processing.InputReader
import spark.TestSparkSession

class InputReaderTest extends FlatSpec with TestSparkSession with Loggable with TestLoggerInitializer {

  "readCitiesTest" should "return a dataframe with 2 columns and not empty" in {

    val source: String = getClass.getResource("/configuration/test-configuration.conf").toString()

    val configuration = ConfigurationReader.readFromPath(source)

    val inputCitiesDF: DataFrame = InputReader.readCities(
      configuration.inputFiles.basePath + configuration.inputFiles.inputCities
    )

    val count: Long = inputCitiesDF.count()
    logger.info(s"count: ${count}")

    assertResult(2794888) {
      count
    }

    val columns: Int = inputCitiesDF.schema.length
    logger.info(s"columns:${columns}")

    assertResult(2) {
      columns
    }

  }

  "cleanCitiesTest" should "return a dataframe with 18713 lines and 3 columns" in {

    val source: String = getClass.getResource("/configuration/test-configuration.conf").toString()

    val configuration = ConfigurationReader.readFromPath(source)

    val inputCitiesDF: DataFrame = InputReader.readCities(
      configuration.inputFiles.basePath + configuration.inputFiles.inputCities)
      .transform(InputReader.cleanCities)

    val count: Long = inputCitiesDF.count()
    logger.info(s"count: ${count}")

    assertResult(18713) {
      count
    }

    val columns: Int = inputCitiesDF.schema.length
    //inputCitiesDF.printSchema()
    //inputCitiesDF.show(false)
    logger.info(s"columnsCount:${columns}")

    assertResult(3) {
      columns
    }

  }
}
