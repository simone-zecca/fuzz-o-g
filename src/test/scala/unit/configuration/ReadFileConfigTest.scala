package unit.configuration

import configuration._
import logging.{Loggable, TestLoggerInitializer}
import org.scalatest.FlatSpec

class ReadFileConfigTest extends FlatSpec with Loggable with TestLoggerInitializer {

  "ReadFileConfigTest" should "return a Configuration object from Hocon file" in {

    val source: String = getClass.getResource("/configuration/test-configuration.conf").toString()

    val config = ConfigurationReader.readFromPath(source)

    assertResult(true) {
      config match {
        case ok: Configuration => true
        case _ => false
      }
    }

    assertResult(true) {
      config.inputFiles match {
        case ok: InputFiles => true
        case _ => false
      }
    }

    assertResult(true) {
      config.geoData match {
        case ok: GeoData => true
        case _ => false
      }
    }

    assertResult(true) {
      config.processing match {
        case ok: Processing => true
        case _ => false
      }
    }

  }

}
