import org.apache.log4j.{Level, LogManager}

/**
  * Created by zaoldyeck on 2015/12/29.
  */
object Logger extends Serializable {
  val log = LogManager.getRootLogger
  log.setLevel(Level.WARN)
}