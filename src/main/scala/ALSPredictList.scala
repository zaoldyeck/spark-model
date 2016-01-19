import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD

import scala.sys.process._

/**
  * Created by zaoldyeck on 2016/1/12.
  */
class ALSPredictList extends ALSFold {

  private val TrainingDataPath: String = "s3n://data.emr/als_play_filter_web_login_days.csv"
  private val PredictDataPath: String = "s3n://data.emr/unique_id_hasid.csv"
  private val OutputPath: String = "/home/hadoop/output/als-for-play.txt"

  override def run(implicit sc: SparkContext): Unit = {
    val trainingRDD: RDD[Rating] = mappingData(sc.textFile(TrainingDataPath)).persist
    val predictRDD: RDD[(Int, Int)] = mappingData2(sc.textFile(TrainingDataPath)).persist
    /*
    val predictRDD: RDD[(Int, Int)] = dropHeader(sc.textFile(PredictDataPath)) flatMap {
      _.split(",") match {
        case Array(uniqueId) => Some((uniqueId.toInt, 90))
        case some =>
          Logger.log.warn("data error:" + some.mkString(","))
          None
      }
    } persist()
    */
    Logger.log.warn("Training Size:" + trainingRDD.count)
    Logger.log.warn("Predict Size:" + predictRDD.count)

    /*
    case class AlsParameters(rank: Int = 10, lambda: Double = 0.01, alpha: Double = 0.01)

    val parametersSeq: IndexedSeq[AlsParameters] = for {
      rank <- 2 until 50 by 1
      lambda <- 0.1 until 15 by 0.1
      alpha <- 0.1 until 50 by 0.1
    } yield new AlsParameters(rank, lambda, alpha)
    */

    val result: RDD[Rating] = new ALS()
      .setImplicitPrefs(true)
      .setRank(10)
      .setIterations(20)
      .setLambda(0.01)
      .setAlpha(0.01)
      .setNonnegative(true)
      .run(trainingRDD).predict(predictRDD)

    val delete_out_path: String = "hadoop fs -rm -f -r ./als-play"
    delete_out_path.!

    //predictResult.saveAsTextFile("./als-play")
    //val evaluation: ConfusionMatrixResult = calConfusionMatrix(predictResult)
    val header: String = "%6d,%2d,%07.4f,%07.4f".format(0, 10, 0.01, 0.01)
    Logger.log.warn("Single:" + header + "," + result.filter(_.rating > 0).count)
    //write(OutputPath, header + "," + evaluation.toListString)
  }


  override def mappingData(data: RDD[String]): RDD[Rating] = {
    Logger.log.warn("Mapping...")

    dropHeader(data) flatMap {
      _.split(",") match {
        case Array(unique_id, game_index, max_login_times, max_login_days, max_duration_sec, pay_times, saving, gender, theme, style, community, type1, type2, web_mobile) =>
          if (max_login_times.toInt >= 2) {
            val rating = max_login_days.toDouble
            Some(Rating(unique_id.toInt, game_index.toInt, if (rating >= 2) 1 else 0))
          } else None
        case some =>
          Logger.log.warn("data error:" + some.mkString(","))
          None
      }
    }
  }

  def mappingData2(data: RDD[String]): RDD[(Int, Int)] = {
    Logger.log.warn("Mapping...")

    dropHeader(data) flatMap {
      _.split(",") match {
        case Array(unique_id, game_index, max_login_times, max_login_days, max_duration_sec, pay_times, saving, gender, theme, style, community, type1, type2, web_mobile) =>
          if (max_login_times.toInt >= 2) {
            Some((unique_id.toInt, 90))
          } else None
        case some =>
          Logger.log.warn("data error:" + some.mkString(","))
          None
      }
    }
  }
}