import java.util.concurrent.Executors

import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

import scala.compat.Platform
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import scala.sys.process._
import scala.util.Random

/**
  * Created by zaoldyeck on 2016/1/12.
  */
class ALSDataSplitTwoTest extends ALSFold {
  private val DataPath: String = "s3n://data.emr/web_user_game_nums.csv"
  private val OutputPath: String = "/home/hadoop/output/als-saving-90.csv"

  override def run(implicit sc: SparkContext): Unit = {
    val executorService: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(2))
    val allRDD: RDD[Rating] = mappingData(sc.textFile(DataPath)).persist
    Logger.log.warn("Total Size:" + allRDD.count)
    val targetGameRDD: RDD[Rating] = allRDD.filter(_.product == 90)
    val notTargetRDD: RDD[Rating] = allRDD.filter(_.product != 90)

    //The Best:14,37,10.4000,05.9000,0.5916,0.4267,0.4790,0.3477,0.4790,0.6523,0.4514
    val parametersSeq: IndexedSeq[AlsParameters] = for {
      rank <- 24 until 68 by 2
      lambda <- logSpace(0.01, 0.42, 100)
      alpha <- logSpace(0.01, 46, 100)
    } yield new AlsParameters(rank, 100, lambda, alpha)

    val futures: IndexedSeq[Future[Unit]] = Random.shuffle(parametersSeq).zipWithIndex map {
      case (parameters, index) =>
        Future {
          val targetGameSplit: Array[RDD[Rating]] = targetGameRDD.randomSplit(Array(0.9, 0.1), Platform.currentTime)
          val targetGameTraining: RDD[Rating] = targetGameSplit(0).persist
          val targetGameTest: RDD[Rating] = targetGameSplit(1).persist
          Logger.log.warn("targetGameTraining Size:" + targetGameTraining.count)
          Logger.log.warn("targetGameTest Size:" + targetGameTest.count)

          val notTargetSplit: Array[RDD[Rating]] = notTargetRDD.randomSplit(Array(0.999, 0.001), Platform.currentTime)
          val notTargetTraining: RDD[Rating] = notTargetSplit(0).repartition(100).persist
          val notTargetTest: RDD[Rating] = notTargetSplit(1).persist
          Logger.log.warn("notTargetTraining Size:" + notTargetTraining.count)
          Logger.log.warn("notTargetTest Size:" + notTargetTest.count)

          Logger.log.warn("Predict...")

          try {
            val model: MatrixFactorizationModel = ALS.trainImplicit(targetGameTraining union notTargetTraining, parameters.rank, parameters.iterations, parameters.lambda, parameters.alpha)

            /*
        new ALS()
          .setImplicitPrefs(true)
          .setRank(parameters.rank)
          .setIterations(50)
          .setLambda(parameters.lambda)
          .setAlpha(parameters.alpha)
          .setNonnegative(true)
          .run(targetGameTraining union notTargetTraining)
        */

            val predictResult1: RDD[PredictResult] = model
              .predict(targetGameTest.map(dataSet => (dataSet.user, dataSet.product)))
              .map(predict => ((predict.user, predict.product), predict.rating))
              .join(targetGameTest.map(dataSet => ((dataSet.user, dataSet.product), dataSet.rating))) map {
              case ((user, product), (predict, fact)) => PredictResult(user, product, predict, fact)
            }

            val predictResult2: RDD[PredictResult] = model
              .predict(notTargetTest.map(dataSet => (dataSet.user, dataSet.product)))
              .map(predict => ((predict.user, predict.product), predict.rating))
              .join(notTargetTest.map(dataSet => ((dataSet.user, dataSet.product), dataSet.rating))) map {
              case ((user, product), (predict, fact)) => PredictResult(user, product, predict, fact)
            }

            val delete_out_path1: String = "hadoop fs -rm -f -r ./als-saving-90"
            delete_out_path1.!

            val delete_out_path2: String = "hadoop fs -rm -f -r ./als-saving-not-90"
            delete_out_path2.!

            predictResult1.saveAsTextFile("./als-saving-78")
            predictResult2.saveAsTextFile("./als-saving-not-78")
            val evaluation1: ConfusionMatrixResult = calConfusionMatrix(predictResult1)
            val evaluation2: ConfusionMatrixResult = calConfusionMatrix(predictResult2)
            val header: String = "%6d,%2d,%07.4f,%07.4f".format(index, parameters.rank, parameters.lambda, parameters.alpha)
            Logger.log.warn("Single:" + header + ",  " + evaluation1.toListString)
            Logger.log.warn("Single:" + header + ",  " + evaluation2.toListString)
            write(OutputPath, header + "," + evaluation1.toListString)
            write(OutputPath, header + "," + evaluation2.toListString)
          } catch {
            case e: Exception => Logger.log.warn(e.printStackTrace())
          }
        }(executorService)
    }
    Await.result(Future.sequence(futures), Duration.Inf)
  }

  override def mappingData(data: RDD[String]): RDD[Rating] = {
    Logger.log.warn("Mapping...")

    dropHeader(data) flatMap {
      _.split(",") match {
        case Array(pub_id, game_id, str_saving, _*) =>
          val rating: Double = str_saving.toDouble
          Some(Rating(pub_id.toInt, game_id.toInt, if (rating > 0) 1 else 0))
        case some =>
          Logger.log.warn("data error:" + some.mkString(","))
          None
      }
    }
  }
}