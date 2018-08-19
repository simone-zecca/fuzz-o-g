package unit.logging

import logging.{Loggable, TestLoggerInitializer}
import org.scalatest.FlatSpec

class LoggerTest extends FlatSpec with Loggable with TestLoggerInitializer {

  "LoggerTest" should "log something with no errors" in {

    logger.info("hello I am a log message")

  }

}
