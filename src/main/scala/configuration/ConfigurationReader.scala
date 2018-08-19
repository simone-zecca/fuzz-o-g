package configuration

import com.typesafe.config.ConfigFactory
import logging.Loggable
import pureconfig.loadConfig

class ConfigurationReader {}

object ConfigurationReader extends Loggable {

  def readFromPath(path: String): Configuration = {

    val configuration = scala.io.Source.fromURL(path).mkString

    loadConfig[Configuration](ConfigFactory.parseString(configuration)) match {
      case Right(config) => {
        logger.info(s"config: ${config}")
        config
      }
      case Left(errors) => throw new RuntimeException(s"Invalid configuration: $errors")
    }

  }

}
