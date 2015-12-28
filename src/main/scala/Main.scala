/**
  * Created by zaoldyeck on 2015/12/25.
  */

import org.apache.log4j.{Level, LogManager}
import org.apache.spark._

object Main {

  def main(args: Array[String]) {
    val sc = setSparkEnv()
    sc.setCheckpointDir("checkpoint")
    LogManager.getRootLogger.setLevel(Level.WARN)
    //new ALSModel().run(sc)
    //new KMeansModel().run(sc)
    new LDAModel().run(sc)
    sc.stop()
  }

  def setSparkEnv(): SparkContext = {
    val conf = new SparkConf().setAppName("SparkAls")
    new SparkContext(conf)
  }
}