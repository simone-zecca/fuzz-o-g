package logging

trait TestLoggerInitializer {

  LoggerLoader.loadConfigurationFromFile(
    configurationFile = "logging.properties",
    loggerOutputPath = "/home/npodevkit/zeppelin_0.7.3/share/FUZZ-OGRAPHY/test-logger.log"
  )

}
