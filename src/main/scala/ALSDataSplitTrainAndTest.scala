import java.io.{FileOutputStream, PrintWriter}

import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD

import scala.compat.Platform
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Random

/**
  * Created by zaoldyeck on 2016/1/12.
  */
class ALSDataSplitTrainAndTest extends ALSFold {

  private val DataPath: String = "s3n://data.emr/not_only_90.csv"
  private val OutputPath: String = "/home/hadoop/output/not-only-90.txt"

  override def run(implicit sc: SparkContext): Unit = {
    val rdd: RDD[Rating] = mappingData(sc.textFile(DataPath)).persist
    val rddNot90: RDD[Rating] = rdd.filter(_.product != 90)
    val rdd90: RDD[Rating] = rdd.filter(_.product == 90)
    Logger.log.warn("Total Size:" + rdd.count)
    Logger.log.warn("Not 90 Size:" + rddNot90.count)
    Logger.log.warn("90 Size:" + rdd90.count)

    val parametersSeq: IndexedSeq[AlsParameters] = for {
      rank <- 2 until 50 by 1
      lambda <- 0.1 until 15 by 0.1
      alpha <- 0.1 until 50 by 0.1
    } yield new AlsParameters(rank, 30, lambda, alpha)

    val futures: IndexedSeq[Future[Any]] = Random.shuffle(parametersSeq).zipWithIndex map {
      case (parameters, index) => Future {
        rdd90.randomSplit(Array(0.99, 0.01), Platform.currentTime) match {
          case Array(training, prediction) =>
            Logger.log.warn("Training Size:" + training.count)
            Logger.log.warn("Predicting Size:" + prediction.count)
            Logger.log.warn("Predict...")
            evaluateModel(rddNot90 union training, prediction, parameters).onSuccess {
              case evaluation =>
                val header: String = "%2d,%2d,%07.4f,%07.4f".format(index, parameters.rank, parameters.lambda, parameters.alpha)
                write(OutputPath, header + "," + evaluation.output)
            }
        }
      }
    }
    Await.result(Future.sequence(futures), Duration.Inf)
  }

  override def mappingData(data: RDD[String]): RDD[Rating] = {
    Logger.log.warn("Mapping...")

    dropHeader(data) flatMap {
      _.split(",") match {
        case Array(pub_id, game_id, saving, play_game_count, is_play_90) =>
          val gameIdNoQuotes = game_id.replace("\"", "")
          val rating = saving.toDouble
          Some(Rating(pub_id.toInt, gameIdNoQuotes.toInt, if (rating > 0) 1 else 0))
        case some =>
          Logger.log.warn("data error:" + some.mkString(","))
          None
      }
    }
  }
}