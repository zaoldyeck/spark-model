/**
  * Created by zaoldyeck on 2015/12/25.
  */

import org.apache.log4j.{Level, LogManager}
import org.apache.spark._

object Main {
  def main(args: Array[String]) {
    LogManager.getRootLogger.setLevel(Level.WARN)
    val conf: SparkConf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.executor.cores", "16")
      .set("spark.executor.memory", "24576m")
      .set("spark.yarn.executor.memoryOverhead", "8192")
    conf.registerKryoClasses(Array(classOf[ALSDataSplitTwoTest], classOf[ALSPredictList], classOf[KMeansSample]))
    implicit val sc: SparkContext = new SparkContext(conf)
    sc.setCheckpointDir("checkpoint")
    /*
    args(0) match {
      case "find" =>
        val gameId: Int = args(1).toInt
        new ALSDataSplitTwoTest(gameId).run
      case "produce" =>
        new ALSPredictList(78, "model_als_game78_login_rate/10_100_0.03025980329605243_0.05044339413434923").run
      case _ =>
    }
    */
    //new ALSModel().run
    //new ALSModel2().run
    //new ALSModel3().run
    //new ALSModel4().run
    //new ALSModel5().run:
    //new ALSModelPredict().run
    //new ALSDataSplitTwoTest(90).run
    //new ALSPredictList(90, "model_als_game90_login_rate/5_30_100_0.25445360117932936_0.09970613948657674").run
    //new ALSPredictPlay(4).run
    new KMeansSample
    //new SVD
    //new LDAModel().run
    //new TestALSModel().run
    //new FormatData().run
    sc.stop()
  }
}