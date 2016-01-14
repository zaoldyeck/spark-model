import java.io.{FileOutputStream, PrintWriter}

import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.compat.Platform
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Random

/**
  * Created by zaoldyeck on 2016/1/12.
  */
class ALSModel6 extends ALSModel3 {

  private val DataPath: String = "s3n://data.emr/als_for_play.csv"
  private val OutputPath: String = "/home/hadoop/output/als-for-play.txt"

  override def run(implicit sc: SparkContext): Unit = {
    val all: RDD[Rating] = mappingData(sc.textFile(DataPath)).persist
    Logger.log.warn("Total Size:" + all.count)

    val parametersSeq: IndexedSeq[AlsParameters] = for {
      rank <- 2 until 50 by 1
      lambda <- 0.1 until 15 by 0.1
      alpha <- 0.1 until 50 by 0.1
    } yield new AlsParameters(rank, 20, lambda, alpha)

    val futures: IndexedSeq[Future[Any]] = Random.shuffle(parametersSeq).zipWithIndex.take(1) map {
      case (parameters, index) => Future {
        all.randomSplit(Array(0.75, 0.25), Platform.currentTime) match {
          case Array(training, prediction) =>
            Logger.log.warn("Training Size:" + training.count)
            Logger.log.warn("Predicting Size:" + prediction.count)
            Logger.log.warn("Predict...")

            val predictResult: RDD[PredictResult] = new ALS()
              .setImplicitPrefs(true)
              .setRank(parameters.rank)
              .setIterations(20)
              .setLambda(parameters.lambda)
              .setAlpha(parameters.alpha)
              .setNonnegative(true)
              .run(training).predict(prediction.map(dataSet => (dataSet.user, dataSet.product)))
              .map(predict => ((predict.user, predict.product), predict.rating))
              .join(prediction.map(dataSet => ((dataSet.user, dataSet.product), dataSet.rating))) map {
              case ((user, product), (predict, fact)) => PredictResult(user, product, predict, fact)
            }

            predictResult.saveAsTextFile("./als-play")
            val evaluation: ConfusionMatrixResult = calConfusionMatrix(predictResult)
            val header: String = "%2d,%2d,%07.4f,%07.4f".format(index, parameters.rank, parameters.lambda, parameters.alpha)
            Logger.log.warn("Single:" + evaluation.toListString)
            write(OutputPath, header + "," + evaluation.toListString)
        }
      }
    }
    Await.result(Future.sequence(futures), Duration.Inf)
  }

  override def mappingData(data: RDD[String]): RDD[Rating] = {
    Logger.log.warn("Mapping...")

    dropHeader(data) flatMap {
      _.split(",") match {
        case Array(pub_id, game_id, login_days, rating) =>
          val gameIdNoQuotes = game_id.replace("\"", "")
          Some(Rating(pub_id.toInt, gameIdNoQuotes.toInt, rating.toDouble))
        case some =>
          Logger.log.warn("data error:" + some.mkString(","))
          None
      }
    }
  }
}