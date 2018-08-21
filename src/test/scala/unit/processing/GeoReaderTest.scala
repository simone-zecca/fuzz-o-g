package unit.processing

import configuration.ConfigurationReader
import logging.{Loggable, TestLoggerInitializer}
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec
import processing.{GeoReader, InputReader}
import spark.TestSparkSession

class GeoReaderTest extends FlatSpec with TestSparkSession with Loggable with TestLoggerInitializer {

  "readGeoCitiesTest" should "return a dataframe with 2894014 lines and 2 columns" in {

    val source: String = getClass.getResource("/configuration/test-configuration.conf").toString()

    val configuration = ConfigurationReader.readFromPath(source)

    val geoCitiesDF: DataFrame = GeoReader.readGeoCities(
      configuration.geoData.basePath + configuration.geoData.geoCities
    )

    val count: Long = geoCitiesDF.count()
    logger.info(s"count: ${count}")

    assertResult(2894014) {
      count
    }

    val columns: Int = geoCitiesDF.schema.length
    logger.info(s"columns:${columns}")

    assertResult(2) {
      columns
    }

  }

  "cleanGeoCitiesTest" should "return a dataframe with 18713 lines and 3 columns" in {

    val source: String = getClass.getResource("/configuration/test-configuration.conf").toString()

    val configuration = ConfigurationReader.readFromPath(source)

    val geoCitiesDF: DataFrame = GeoReader.readGeoCities(
      configuration.geoData.basePath + configuration.geoData.geoCities).transform(GeoReader.cleanGeoCities)

    val count: Long = geoCitiesDF.count()
    logger.info(s"count: ${count}")

    assertResult(2894014) {
      count
    }

    val columns: Int = geoCitiesDF.schema.length
    logger.info(s"columnsCount:${geoCitiesDF.schema}")
    //geoCitiesDF.show(false)
    logger.info(s"columnsCount:${columns}")

    assertResult(3) {
      columns
    }

  }
}
