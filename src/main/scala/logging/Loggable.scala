package logging

import org.apache.log4j.Logger

trait Loggable {

  protected def logger: Logger = Logger.getLogger(this.getClass)

}
