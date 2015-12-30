import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD

import scala.sys.process._

/**
  * Created by zaoldyeck on 2015/12/30.
  */
class ALSModel2 extends ALSModel {
  private val TRAINING_DATA_IN_PATH = "hdfs://pubgame/user/vincent/pg_user_game_90_training_has_login_days.csv"
  private val TEST_DATA_IN_PATH = "hdfs://pubgame/user/vincent/pg_user_game_90_test_has_login_days.csv"
  private val OUTPUT_PATH = "hdfs://pubgame/user/vincent/spark-als2"

  override def run(sc: SparkContext): Unit = {

    // Load and parse the data
    Logger.log.warn("Load into RDD...")
    val trainingData: RDD[String] = sc.textFile(TRAINING_DATA_IN_PATH)
    val testData: RDD[String] = sc.textFile(TEST_DATA_IN_PATH)
    //val ratings: SparkRDD[Rating] = ratingData(mappingData(trainingData))

    val ratings: RDD[Rating] = ratingData(trainingData)
    val ratingsTest: RDD[Rating] = mappingData(testData)
    Logger.log.warn("Training Data Size=" + ratings.count)
    Logger.log.warn("Test Data Size=" + ratingsTest.count)

    // Build the recommendation model using ALS
    val rank = 10 //number of lantent factors
    val numIterations = 5
    val lambda = 0.01 //normalization parameter
    val alpha = 1.0

    Logger.log.warn("Training...")
    val model = ALS.trainImplicit(ratings, rank, numIterations, lambda, alpha)

    // Evaluate the model on rating data
    val usersProducts = ratingsTest.map {
      case Rating(user, product, rate) => (user, product)
    }

    Logger.log.warn("Predicting...")
    val predictions = model.predict(usersProducts).map {
      case Rating(user, product, rate) => ((user, product), rate)
    }
    Logger.log.warn("Predictions Size=" + predictions.count)

    Logger.log.warn("Joining...")
    val ratesAndPreds = ratingsTest.map {
      case Rating(user, product, rate) => ((user, product), rate)
    } join predictions sortByKey() //ascending or descending

    Logger.log.warn("Try to delete path: [" + OUTPUT_PATH + "]")
    val delete_out_path = "hadoop fs -rm -f -r " + OUTPUT_PATH
    delete_out_path.!

    val formatedRatesAndPreds = ratesAndPreds.map {
      case ((user, product), (rate, pred)) =>
        val output = user + "\t" + product + "\t" + rate + "\t" + "%02.4f" format pred
        Logger.log.warn("output=" + output)
        output
    }
    formatedRatesAndPreds.saveAsTextFile(OUTPUT_PATH)

    val MSE = ratesAndPreds.map {
      case ((user, product), (r1, r2)) =>
        val err = r1 - r2
        err * err
    } mean()

    Logger.log.warn("--->Mean Squared Error = " + MSE)
    Logger.log.warn(calConfusionMatrix(ratesAndPreds).toString)
  }

  def ratingData(data: RDD[String]): RDD[Rating] = {
    Logger.log.warn("Mapping...")

    case class Data(pubId: Int, gameId: Int, loginDays: Int, saving: Int)

    val parseData = data.filter(_ != data.first).map(_.split(",") match {
      case Array(pub_id, game_id, login_days, saving) => Data(pub_id.toInt, game_id.toInt, login_days.toInt, saving.toInt)
    })
    val dataSize = parseData.count
    val partSize = dataSize / 5
    val sortByLoginDays = parseData.sortBy(_.loginDays)
    val sortBySaving = parseData.sortBy(_.saving)
    val loginDaysLevel1 = sortByLoginDays.zipWithIndex.filter(_._2 == partSize).first._1.loginDays
    val loginDaysLevel2 = sortByLoginDays.zipWithIndex.filter(_._2 == partSize * 2).first._1.loginDays
    val loginDaysLevel3 = sortByLoginDays.zipWithIndex.filter(_._2 == partSize * 3).first._1.loginDays
    val loginDaysLevel4 = sortByLoginDays.zipWithIndex.filter(_._2 == partSize * 4).first._1.loginDays
    val savingLevel1 = sortBySaving.zipWithIndex.filter(_._2 == partSize).first._1.saving
    val savingLevel2 = sortBySaving.zipWithIndex.filter(_._2 == partSize * 2).first._1.saving
    val savingLevel3 = sortBySaving.zipWithIndex.filter(_._2 == partSize * 3).first._1.saving
    val savingLevel4 = sortBySaving.zipWithIndex.filter(_._2 == partSize * 4).first._1.saving

    parseData map {
      case row =>
        val loginScore = row.loginDays match {
          case days if days < loginDaysLevel1 => 1
          case days if days < loginDaysLevel2 => 2
          case days if days < loginDaysLevel3 => 3
          case days if days < loginDaysLevel4 => 4
          case _ => 5
        }
        val savingScore = row.saving match {
          case saving if saving < savingLevel1 => 1
          case saving if saving < savingLevel2 => 2
          case saving if saving < savingLevel3 => 3
          case saving if saving < savingLevel4 => 4
          case _ => 5
        }
        Rating(row.pubId, row.gameId, loginScore + savingScore * 4)
    }
  }
}