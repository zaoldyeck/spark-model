/**
  * Created by zaoldyeck on 2015/12/25.
  */

import org.apache.log4j.{Level, LogManager}
import org.apache.spark._
import org.apache.spark.launcher.SparkLauncher

object Main {

  def main(args: Array[String]) {
    LogManager.getRootLogger.setLevel(Level.WARN)
    implicit val sc = setSparkEnv()
    sc.setCheckpointDir("checkpoint")
    //new ALSModel().run
    //new ALSModel2().run
    new ALSModel3().run()
    //new KMeansModel().run
    //new LDAModel().run
    //new TestALSModel().run
    //new FormatData().run
    sc.stop()
  }

  def setSparkEnv(): SparkContext = {
    val conf = new SparkConf().setAppName("SparkAls").set(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS,
      "-Dcom.sun.management.jmxremote.port=12345 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote")
    new SparkContext(conf)
  }
}