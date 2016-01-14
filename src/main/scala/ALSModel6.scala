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

    val futures: IndexedSeq[Future[Any]] = Random.shuffle(parametersSeq).zipWithIndex map {
      case (parameters, index) => Future {
        all.randomSplit(Array(0.75, 0.25), Platform.currentTime) match {
          case Array(training, prediction) =>
            Logger.log.warn("Training Size:" + training.count)
            Logger.log.warn("Predicting Size:" + prediction.count)
            Logger.log.warn("Predict...")
            val predictResult: RDD[PredictResult] = ALS.trainImplicit(training, 20, parameters.rank, parameters.lambda, parameters.alpha)
              .predict(prediction.map(rating => (rating.user, rating.product)))
              .map(predict => ((predict.user, predict.product), predict.rating))
              .join(prediction.map(result => ((result.user, result.product), result.rating))) map {
              case ((user, product), (predict, fact)) => PredictResult(user, product, predict, fact)
            }
            predictResult.saveAsTextFile("./als-all")
            val header: String = "%2d,%2d,%07.4f,%07.4f".format(index, parameters.rank, parameters.lambda, parameters.alpha)
            val result: ConfusionMatrixResult = calConfusionMatrix(predictResult)
            Logger.log.warn(result.toString)
            write(OutputPath, header + "," + result.toListString)
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