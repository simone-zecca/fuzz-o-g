package logging

import java.util.Properties

import org.apache.log4j.{Level, Logger, PropertyConfigurator}

/** Provides LoggerLoader loaded by a configuration file.
 */
object LoggerLoader {

  def loadConfigurationFromFile(configurationFile: String,
                                loggerOutputPath: String,
                                rootLevel: Option[String] = None): Unit = {
    val properties = loadPropertiesFromConfigurationFile(configurationFile)
    configureTimestampAppender(loggerOutputPath, properties)
    configureRootLogger(rootLevel, properties)
    applyConfiguration(properties)
  }

  private def loadPropertiesFromConfigurationFile(configurationFile: String): Properties = {
    val configuration = readConfiguration(configurationFile)
    val properties = new Properties
    properties.load(configuration)
    properties
  }

  private def readConfiguration(configurationFile: String) = {
    this.getClass.getClassLoader
      .getResourceAsStream(configurationFile)
  }

  private def configureTimestampAppender(path: String, properties: Properties): Unit =
    properties.setProperty("log4j.appender.TimestampAppender.File", path)

  private def configureRootLogger(rootLevel: Option[String], properties: Properties): Unit =
    rootLevel match {
      case Some(lev) =>
        properties
          .setProperty("log4j.rootLogger", lev + ", console, TimestampAppender")
      case _ =>
    }

  private def applyConfiguration(properties: Properties): Unit =
    PropertyConfigurator.configure(properties)

  def setSparkLogLevel(level: String): Unit = {
    Logger.getLogger("org").setLevel(Level.toLevel(level, Level.ERROR))
    Logger.getLogger("akka").setLevel(Level.toLevel(level, Level.ERROR))
  }

  def getLogger(clazz: Class[_]): Logger = Logger.getLogger(clazz)
}
