import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD

import scala.sys.process._

/**
  * Created by zaoldyeck on 2015/12/30.
  */
class ALSModel2(implicit sc: SparkContext) extends ALSModel {
  private val TRAINING_DATA_IN_PATH = "s3n://data.emr/pg_user_game_90_training_v3.csv"
  private val TEST_DATA_IN_PATH = "s3n://data.emr/pg_user_game_90_other.csv"
  private val OUTPUT_PATH = "./spark-als2"

  override def run(implicit sc: SparkContext): Unit = {

    // Load and parse the data
    Logger.log.warn("Load into RDD...")
    val trainingData: RDD[String] = sc.textFile(TRAINING_DATA_IN_PATH)
    val testData: RDD[String] = sc.textFile(TEST_DATA_IN_PATH)
    //val ratings: SparkRDD[Rating] = ratingData(mappingData(trainingData))

    val ratings: RDD[Rating] = ratingData(trainingData)
    val ratingsTest: RDD[TestData] = mappingTestData(testData)
    Logger.log.warn("Training Data Size=" + ratings.count)
    Logger.log.warn("Test Data Size=" + ratingsTest.count)

    // Build the recommendation model using ALS
    val rank = 8 //number of lantent factors
    val numIterations = 50
    val lambda = 0.01 //normalization parameter
    val alpha = 0.01

    Logger.log.warn("Training...")
    //val model = ALS.trainImplicit(ratings, rank, numIterations, lambda, alpha)
    val model = ALS.train(ratings, rank, numIterations, lambda)

    // Evaluate the model on rating data
    val usersProducts: RDD[(Int, Int)] = ratingsTest.map {
      case TestData(user, product, rate, score) => (user, product)
    }

    Logger.log.warn("Predicting...")
    val predictions: RDD[((Int, Int), Double)] = model.predict(usersProducts).map {
      case Rating(user, product, rate) => ((user, product), rate)
    }
    Logger.log.warn("Predictions Size=" + predictions.count)

    Logger.log.warn("Joining...")
    val ratesAndPreds: RDD[((Int, Int), ((Int, Double), Double))] = ratingsTest.map {
      case TestData(user, product, rate, score) => ((user, product), (rate, score))
    } join predictions sortByKey() //ascending or descending

    Logger.log.warn("Try to delete path: [" + OUTPUT_PATH + "]")
    val delete_out_path = "hadoop fs -rm -f -r " + OUTPUT_PATH
    delete_out_path.!

    val formatedRatesAndPreds = ratesAndPreds.map {
      case ((user, product), ((rate, score), pred)) =>
        val output = user + "\t" + product + "\t" + rate + "\t" + score + "\t" + "%02.4f" format pred
        //Logger.log.warn("output=" + output)
        output
    }
    formatedRatesAndPreds.saveAsTextFile(OUTPUT_PATH)

    val MSE = ratesAndPreds.map {
      case ((user, product), ((rate, score), pred)) =>
        val err = rate - pred
        err * err
    } mean()

    Logger.log.warn("--->Mean Squared Error = " + MSE)
    Logger.log.warn(calConfusionMatrix(ratesAndPreds map {
      case ((user, product), ((rate, score), pred)) => ((user, product), (rate.toDouble, pred))
    }))
  }

  private def ratingData(data: RDD[String]): RDD[Rating] = {
    Logger.log.warn("Rating...")

    case class Data(pubId: Int, gameId: Int, loginDays: Int, saving: Int)
    val header: String = data.first
    val parseData = data.filter(_ != header).map(_.split(",") match {
      case Array(pub_id, game_id, login_days, saving) => Data(pub_id.toInt, game_id.toInt, login_days.toInt, saving.toInt)
    })
    val dataSize = parseData.count
    val partSize = (dataSize / 5).toInt
    val sortByLoginDays = parseData.sortBy(_.loginDays).toLocalIterator.toList
    val sortBySaving = parseData.sortBy(_.saving).toLocalIterator.toList
    val loginDaysLevel1 = sortByLoginDays(partSize - 1).loginDays
    val loginDaysLevel2 = sortByLoginDays(partSize * 2 - 1).loginDays
    val loginDaysLevel3 = sortByLoginDays(partSize * 3 - 1).loginDays
    val loginDaysLevel4 = sortByLoginDays(partSize * 4 - 1).loginDays
    val savingLevel1 = sortBySaving(partSize - 1).saving
    val savingLevel2 = sortBySaving(partSize * 2 - 1).saving
    val savingLevel3 = sortBySaving(partSize * 3 - 1).saving
    val savingLevel4 = sortBySaving(partSize * 4 - 1).saving

    parseData map {
      case row =>
        val loginScore = row.loginDays match {
          case days if days <= loginDaysLevel1 => 1
          case days if days <= loginDaysLevel2 => 2
          case days if days <= loginDaysLevel3 => 3
          case days if days <= loginDaysLevel4 => 4
          case _ => 5
        }
        val savingScore = row.saving match {
          case saving if saving <= savingLevel1 => 1
          case saving if saving <= savingLevel2 => 2
          case saving if saving <= savingLevel3 => 3
          case saving if saving <= savingLevel4 => 4
          case _ => 5
        }

        val result = Rating(row.pubId, row.gameId, loginScore.toDouble / 100 + savingScore.toDouble)
        //Logger.log.warn("source = " + row)
        //Logger.log.warn("result = " + result)
        //Logger.log.warn("===")
        result
    }
  }

  case class TestData(pubId: Int, gameId: Int, saving: Int, score: Double)

  private def mappingTestData(data: RDD[String]): RDD[TestData] = {
    Logger.log.warn("Mapping Test Data...")

    case class Data(pubId: Int, gameId: Int, loginDays: Int, saving: Int)
    val header: String = data.first
    val parseData = data.filter(_ != header).map(_.split(",") match {
      case Array(pub_id, game_id, login_days, saving) => Data(pub_id.toInt, game_id.toInt, login_days.toInt, saving.toInt)
    })
    val dataSize = parseData.count
    val partSize = (dataSize / 5).toInt
    val sortByLoginDays = parseData.sortBy(_.loginDays).toLocalIterator.toList
    val sortBySaving = parseData.sortBy(_.saving).toLocalIterator.toList
    val loginDaysLevel1 = sortByLoginDays(partSize - 1).loginDays
    val loginDaysLevel2 = sortByLoginDays(partSize * 2 - 1).loginDays
    val loginDaysLevel3 = sortByLoginDays(partSize * 3 - 1).loginDays
    val loginDaysLevel4 = sortByLoginDays(partSize * 4 - 1).loginDays
    val savingLevel1 = sortBySaving(partSize - 1).saving
    val savingLevel2 = sortBySaving(partSize * 2 - 1).saving
    val savingLevel3 = sortBySaving(partSize * 3 - 1).saving
    val savingLevel4 = sortBySaving(partSize * 4 - 1).saving

    parseData map {
      case row =>
        val loginScore = row.loginDays match {
          case days if days <= loginDaysLevel1 => 0
          case days if days <= loginDaysLevel2 => 1
          case days if days <= loginDaysLevel3 => 2
          case days if days <= loginDaysLevel4 => 3
          case _ => 4
        }
        val savingScore = row.saving match {
          case saving if saving <= savingLevel1 => 0
          case saving if saving <= savingLevel2 => 1
          case saving if saving <= savingLevel3 => 2
          case saving if saving <= savingLevel4 => 3
          case _ => 4
        }

        TestData(row.pubId, row.gameId, row.saving, loginScore.toDouble / 100 + savingScore.toDouble)
    }
  }
}