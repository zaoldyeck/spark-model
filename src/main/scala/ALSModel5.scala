import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, ALS, Rating}
import org.apache.spark.rdd.RDD

import scala.compat.Platform
import scala.sys.process._
import scala.util.Random

/**
  * Created by zaoldyeck on 2016/1/12.
  */
class ALSModel5 extends ALSFold {

  private val TrainingDataPath1: String = "s3n://data.emr/double_ckeck_training2.csv"
  //private val TrainingDataPath2: String = "s3n://data.emr/als_play_90_join_training.csv"
  //private val TrainingDataPath: String = "s3n://data.emr/als_play_90_training_sample.csv"
  private val PredictDataPath1: String = "s3n://data.emr/double_check_test_90.csv"
  private val PredictDataPath2: String = "s3n://data.emr/double_ckeck_test_not_90_2.csv"
  private val OutputPath: String = "/home/hadoop/output/als-play.txt"

  override def run(implicit sc: SparkContext): Unit = {
    val split: Array[RDD[Rating]] = mappingData(sc.textFile(TrainingDataPath1)).randomSplit(Array(0.8, 0.1, 0.1), Platform.currentTime)
    lazy val trainingRDD1: RDD[Rating] = split(0) //mappingData(sc.textFile(TrainingDataPath1)).persist
    //lazy val trainingRDD2: RDD[Rating] = mappingData(sc.textFile(TrainingDataPath2)).persist
    //lazy val trainingRDD: RDD[Rating] = mappingData(sc.textFile(TrainingDataPath)).persist
    //lazy val validateRDD: RDD[Rating] = mappingData(sc.textFile(PredictDataPath)).persist
    lazy val testRDD1: RDD[Rating] = split(1) //mappingData(sc.textFile(PredictDataPath1)).persist
    lazy val testRDD2: RDD[Rating] = split(2) //mappingData2(sc.textFile(PredictDataPath2)).persist
    //Logger.log.warn("Training Size:" + trainingRDD.count)
    Logger.log.warn("Training1 Size:" + trainingRDD1.count)
    Logger.log.warn("Test1 Size:" + testRDD1.count)
    Logger.log.warn("Test2 Size:" + testRDD2.count)

    //The Best:14,37,10.4000,05.9000,0.5916,0.4267,0.4790,0.3477,0.4790,0.6523,0.4514
    /*
    val parametersSeq: IndexedSeq[AlsParameters] = for {
      rank <- 2 until 100 by 1
      lambda <- 0.1 until 10 by 0.03
      alpha <- 0.1 until 10 by 0.03
    } yield new AlsParameters(rank, 50, lambda, alpha)
    */

    //Random.shuffle(parametersSeq).zipWithIndex foreach {
    //case (parameters, index) =>
    Logger.log.warn("Predict...")

    /*
    val predictResult: RDD[PredictResult] = new ALS()
      .setImplicitPrefs(true)
      .setRank(10)
      .setIterations(20)
      .setLambda(0.01)
      .setAlpha(0.01)
      .setNonnegative(true)
      .run(trainingRDD1 union trainingRDD2).predict(validateRDD.map(dataSet => (dataSet.user, dataSet.product)))
      .map(predict => ((predict.user, predict.product), predict.rating))
      .join(validateRDD.map(dataSet => ((dataSet.user, dataSet.product), dataSet.rating))) map {
      case ((user, product), (predict, fact)) => PredictResult(user, product, predict, fact)
    }
    */
    //val not90split: Array[RDD[Rating]] = trainingRDD1.randomSplit(Array(0.9, 0.1), Platform.currentTime)
    //Logger.log.warn("Training Size:" + not90split(0).count)
    //Logger.log.warn("Test Size:" + not90split(1).count)

    val model: MatrixFactorizationModel = //ALS.trainImplicit(trainingRDD1, 10, 20, 0.01, 0.01)
      new ALS()
        .setImplicitPrefs(true)
        .setRank(10)
        .setIterations(20)
        .setLambda(0.01)
        .setAlpha(0.01)
        .setNonnegative(true)
        .run(trainingRDD1)

    val predictResult: RDD[PredictResult] = model
      .predict(testRDD1.map(dataSet => (dataSet.user, dataSet.product)))
      .map(predict => ((predict.user, predict.product), predict.rating))
      .join(testRDD1.map(dataSet => ((dataSet.user, dataSet.product), dataSet.rating))) map {
      case ((user, product), (predict, fact)) => PredictResult(user, product, predict, fact)
    }

    val predict1: RDD[Rating] = model
      .predict(testRDD2.map(dataSet => (dataSet.user, dataSet.product)))

    Logger.log.warn("predict1 Size:" + predict1.count())

    val predictResult2: RDD[PredictResult] = predict1
      .map(predict => ((predict.user, predict.product), predict.rating))
      .join(testRDD2.map(dataSet => ((dataSet.user, dataSet.product), dataSet.rating))) map {
      case ((user, product), (predict, fact)) => PredictResult(user, product, predict, fact)
    }

    val delete_out_path: String = "hadoop fs -rm -f -r ./als-play"
    delete_out_path.!
    val delete_out_path2: String = "hadoop fs -rm -f -r ./als-play2"
    delete_out_path2.!

    predictResult.saveAsTextFile("./als-play")
    predictResult2.saveAsTextFile("./als-play2")
    val evaluation: ConfusionMatrixResult = calConfusionMatrix(predictResult)
    val evaluation2: ConfusionMatrixResult = calConfusionMatrix(predictResult2)
    val header: String = "%6d,%2d,%07.4f,%07.4f".format(0, 10, 0.01, 0.01)
    Logger.log.warn("Single:" + header + ",  " + evaluation.toListString)
    Logger.log.warn("Single:" + header + ",  " + evaluation2.toListString)
    write(OutputPath, header + "," + evaluation.toListString)
    //}
  }

  override def mappingData(data: RDD[String]): RDD[Rating] = {
    Logger.log.warn("Mapping...")

    dropHeader(data) flatMap {
      _.split(",") match {
        case Array(unique_id, game_index, max_login_times, max_login_days, max_duration_sec, pay_times, saving, gender, theme, style, community, type1, type2, web_mobile) =>
          Some(Rating(unique_id.toInt, game_index.toInt, if (saving.toInt > 0) 1 else 0))
        case some =>
          Logger.log.warn("data error:" + some.mkString(","))
          None
      }
    }
  }

  def mappingData2(data: RDD[String]): RDD[Rating] = {
    Logger.log.warn("Mapping...")

    dropHeader(data) flatMap {
      _.split(",") match {
        case Array(unique_id, game_index, max_login_times, max_login_days, max_duration_sec, pay_times, saving, gender, theme, style, community, type1, type2, web_mobile, play_games, sum_saving) =>
          Some(Rating(unique_id.toInt, game_index.toInt, if (saving.toInt > 0) 1 else 0))
        case some =>
          Logger.log.warn("data error:" + some.mkString(","))
          None
      }
    }
  }

  def mappingData3(data: RDD[String]): RDD[Rating] = {
    Logger.log.warn("Mapping...")

    dropHeader(data) flatMap {
      _.split(",") match {
        case Array(unique_id, game_index, saving_index, saving) =>
          Some(Rating(unique_id.toInt, game_index.toInt, saving_index.toDouble))
        case some =>
          Logger.log.warn("data error:" + some.mkString(","))
          None
      }
    }
  }

  def mappingData4(data: RDD[String]): RDD[Rating] = {
    Logger.log.warn("Mapping...")

    dropHeader(data) flatMap {
      _.split(",") match {
        case Array(unique_id, game_index, saving_index, saving, game_nums, play90) =>
          Some(Rating(unique_id.toInt, game_index.toInt, saving_index.toDouble))
        case some =>
          Logger.log.warn("data error:" + some.mkString(","))
          None
      }
    }
  }
}