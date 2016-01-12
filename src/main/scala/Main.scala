/**
  * Created by zaoldyeck on 2015/12/25.
  */

import org.apache.log4j.{Level, LogManager}
import org.apache.spark._

object Main {

  def main(args: Array[String]) {
    LogManager.getRootLogger.setLevel(Level.WARN)
    implicit val sc: SparkContext = setSparkEnv()
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
    val conf: SparkConf = new SparkConf().setAppName("SparkAls")
    new SparkContext(conf)
  }
}