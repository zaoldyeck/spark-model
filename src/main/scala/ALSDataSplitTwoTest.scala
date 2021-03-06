import java.util.concurrent.Executors

import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.compat.Platform
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import scala.sys.process._
import scala.util.{Failure, Random, Success, Try}

/**
  * Created by zaoldyeck on 2016/1/12.
  */
class ALSDataSplitTwoTest(gameId: Int) extends ALSFold {
  private val DataPath: String = "s3n://data.emr/training_all_login_rate"
  //private val OutputPath: String = s"/home/hadoop/output/als-saving-$gameId.csv"
  //private val OutputPath: String = s"/home/vincent/output/als-saving-$gameId.csv"
  private val OutputPath: String = s"/home/vincent/output/als-login-over1-$gameId.csv"

  override def run(implicit sc: SparkContext): Unit = {
    val executorService: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(25))
    //val allRDD: RDD[Rating] = mappingData(sc.textFile(DataPath, 64)).persist
    //val allRDD: RDD[Rating] = loadSavingDataFromHive("parquet.`/user/kigo/kigo_1456315788061/modeling_data`").persist
    val allRDD: RDD[Rating] = loadLoginDataFromHive("parquet.`/user/kigo/dataset/modeling_data`").persist
    //val allRDD: RDD[Rating] = loadDataFromHive("parquet.`/user/hive/warehouse/kigo_1456315788061.db/modeling_data`").sample(withReplacement = false, 0.01, Platform.currentTime).persist
    Logger.log.warn("Total Size:" + allRDD.count)

    val targetGameRDD: RDD[Rating] = allRDD.filter(_.product == gameId).persist
    val notTargetRDD: RDD[Rating] = allRDD.filter(_.product != gameId).persist
    allRDD.unpersist()

    //The Best:14,37,10.4000,05.9000,0.5916,0.4267,0.4790,0.3477,0.4790,0.6523,0.4514
    val parametersSeq: IndexedSeq[AlsParameters] = for {
      rank <- 2 until 100 by 2
      lambda <- logSpace(0.01, 50, 100)
      alpha <- logSpace(0.01, 50, 100)
    } yield new AlsParameters(rank, 100, lambda, alpha)

    val futures: IndexedSeq[Future[Unit]] = Random.shuffle(parametersSeq).zipWithIndex map {
      case (parameters, index) =>
        Future {
          val targetGameSplit: Array[RDD[Rating]] = targetGameRDD.randomSplit(Array(0.9, 0.1), Platform.currentTime)
          val targetGameTraining: RDD[Rating] = targetGameSplit(0).persist
          val targetGameTest: RDD[Rating] = targetGameSplit(1).persist
          Logger.log.warn("targetGameTraining Size:" + targetGameTraining.count)
          Logger.log.warn("targetGameTest Size:" + targetGameTest.count)

          val notTargetSplit: Array[RDD[Rating]] = notTargetRDD.randomSplit(Array(0.99, 0.01), Platform.currentTime)
          val notTargetTraining: RDD[Rating] = notTargetSplit(0).persist
          val notTargetTest: RDD[Rating] = notTargetSplit(1).persist
          Logger.log.warn("notTargetTraining Size:" + notTargetTraining.count)
          Logger.log.warn("notTargetTest Size:" + notTargetTest.count)

          val trainingData: RDD[Rating] = targetGameTraining.union(notTargetTraining).persist
          //val trainingDataTest: RDD[Rating] = trainingData.sample(withReplacement = false, 0.2, Platform.currentTime).persist
          targetGameTraining.unpersist()
          notTargetTraining.unpersist()

          Logger.log.warn("Training...")

          try {
            val model: MatrixFactorizationModel = ALS.trainImplicit(trainingData, parameters.rank, parameters.iterations, parameters.lambda, parameters.alpha)

            /*
            val model: MatrixFactorizationModel =
              new ALS()
                .setImplicitPrefs(true)
                .setRank(parameters.rank)
                .setIterations(parameters.iterations)
                .setLambda(parameters.lambda)
                .setAlpha(parameters.alpha)
                .setNonnegative(true)
                .run(targetGameTraining union notTargetTraining)
            */
            trainingData.unpersist()

            /*
            val predictResult1: RDD[PredictResult] = model
              .predict(trainingDataTest.map(dataSet => (dataSet.user, dataSet.product)))
              .map(predict => ((predict.user, predict.product), predict.rating))
              .join(trainingDataTest.map(dataSet => ((dataSet.user, dataSet.product), dataSet.rating))) map {
              case ((user, product), (predict, fact)) => PredictResult(user, product, predict, fact)
            }
            trainingDataTest.unpersist()
            */

            val predictResult2: RDD[PredictResult] = model
              .predict(targetGameTest.map(dataSet => (dataSet.user, dataSet.product)))
              .map(predict => ((predict.user, predict.product), predict.rating))
              .join(targetGameTest.map(dataSet => ((dataSet.user, dataSet.product), dataSet.rating))) map {
              case ((user, product), (predict, fact)) => PredictResult(user, product, predict, fact)
            }
            targetGameTest.unpersist()

            val predictResult3: RDD[PredictResult] = model
              .predict(notTargetTest.map(dataSet => (dataSet.user, dataSet.product)))
              .map(predict => ((predict.user, predict.product), predict.rating))
              .join(notTargetTest.map(dataSet => ((dataSet.user, dataSet.product), dataSet.rating))) map {
              case ((user, product), (predict, fact)) => PredictResult(user, product, predict, fact)
            }
            notTargetTest.unpersist()

            /*
            val delete_out_path1: String = s"hadoop fs -rm -f -r als-saving-$gameId"
            delete_out_path1.!
            val delete_out_path2: String = s"hadoop fs -rm -f -r als-saving-not-$gameId"
            delete_out_path2.!
            predictResult2.saveAsTextFile(s"als-saving-$gameId")
            predictResult3.saveAsTextFile(s"als-saving-not-$gameId")
            */

            //val evaluation1: ConfusionMatrixResult = calConfusionMatrix(predictResult1)
            val evaluation2: ConfusionMatrixResult = calConfusionMatrix(predictResult2)
            val evaluation3: ConfusionMatrixResult = calConfusionMatrix(predictResult3)
            val header: String = "%6d,%2d,%07.4f,%07.4f".format(index, parameters.rank, parameters.lambda, parameters.alpha)
            //Logger.log.warn("Single:" + header + ",  " + evaluation1.toListString)
            Logger.log.warn("Single:" + header + ",  " + evaluation2.toListString)
            Logger.log.warn("Single:" + header + ",  " + evaluation3.toListString)
            //write(OutputPath, header + "," + evaluation1.toListString)
            write(OutputPath, header + "," + evaluation2.toListString)
            write(OutputPath, header + "," + evaluation3.toListString)
            //model.save(sc, s"model_als_saving_game$gameId/${index}_${parameters.rank}_${parameters.iterations}_${parameters.lambda}_${parameters.alpha}")
          } catch {
            case e: Exception => Logger.log.warn(e.printStackTrace())
          }
        }(executorService)
    }
    Await.result(Future.sequence(futures), Duration.Inf)
  }

  override def mappingData(data: RDD[String]): RDD[Rating] = {
    Logger.log.warn("Mapping...")

    //dropHeader(data) flatMap {
    data flatMap {
      _.split(",") match {
        //case Array(pub_id, game_id, str_saving, _*) =>
        case Array(unique_id, game_index, max_login_times, max_login_days, max_duration_sec, pay_times, saving, gender, theme, style, community, type1, type2, web_mobile, login_rate) =>
          Try {
            val rating: Double = login_rate.toDouble
            Some(Rating(unique_id.toInt, game_index.toInt, if (max_login_days.toInt > 1 || rating > 1) 1 else 0))
          } match {
            case Success(rate) => rate
            case Failure(e) => None
          }
        case some =>
          Logger.log.warn("data error:" + some.mkString(","))
          None
      }
    }
  }

  def loadSavingDataFromHive(schema: String)(implicit sc: SparkContext): RDD[Rating] = {
    val sqlContext = org.apache.spark.sql.SQLContext.getOrCreate(sc)
    sqlContext.sql(s"select unique_id,game_index,revenue from $schema where web_mobile=2").rdd map {
      case Row(unique_id: Long, game_index: Int, revenue) =>
        Rating(unique_id.toInt, game_index, if (Some(revenue).getOrElse(0).asInstanceOf[Long] > 0) 1 else 0)
    }
  }

  def loadLoginDataFromHive(schema: String)(implicit sc: SparkContext): RDD[Rating] = {
    val sqlContext = org.apache.spark.sql.SQLContext.getOrCreate(sc)
    sqlContext.sql(s"select unique_id,game_index,unique_login_days from $schema where web_mobile=2").rdd map {
      case Row(unique_id: Long, game_index: Int, unique_login_days) =>
        Rating(unique_id.toInt, game_index, if (Some(unique_login_days).getOrElse(0).asInstanceOf[Long] > 1) 1 else 0)
    }
  }
}