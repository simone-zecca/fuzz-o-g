package unit.full

import launcher.FuzzOgraphy
import logging.{Loggable, TestLoggerInitializer}
import org.scalatest.FlatSpec
import spark.TestSparkSession

class FuzzOgraphyTest extends FlatSpec with TestSparkSession with Loggable with TestLoggerInitializer {

  "FuzzOgraphyTest" should "perform full process without errors" in {

    val settingsPath: String = getClass.getResource("/configuration/test-configuration.conf").toString()

    val launcher = FuzzOgraphy(settingsPath)
    launcher.run()

  }

}
