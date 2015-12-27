/**
  * Created by zaoldyeck on 2015/12/25.
  */

import akka.event.slf4j.Logger
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark._
import org.apache.spark.rdd.{RDD => SparkRDD}
import org.slf4j.LoggerFactory
import sys.process._

object Main {
  val sparkLogger = LoggerFactory.getLogger(getClass)
  val akkaLogger = Logger("！！This Is Important Message！！")

  private val OUTPUT_HADOOP_PATH = "hdfs://pubgame/user/vincent/spark-als"
  //private val TRAINING_DATA_IN_PATH = "hdfs://pubgame/user/vincent/pg_with_gd_for_model_with_revenue_training.csv"
  //private val TEST_DATA_IN_PATH = "hdfs://pubgame/user/vincent/pg_with_gd_for_model_with_revenue_testing_inner.csv"
  private val TRAINING_DATA_IN_PATH = "hdfs://pubgame/user/vincent/temp_pg_game_target_game_id_not_90.csv"
  private val TEST_DATA_IN_PATH = "hdfs://pubgame/user/vincent/temp_pg_game_target_game_user_play_90_and_play_another_game.csv"

  def main(args: Array[String]) {
    val sc = setSparkEnv()
    sc.setCheckpointDir("checkpoint")
    LogManager.getRootLogger.setLevel(Level.WARN)
    ExecAls(sc)
    sc.stop()
  }

  def setSparkEnv(): SparkContext = {
    val conf = new SparkConf().setAppName("SparkAls")
    new SparkContext(conf)
  }


  def dropHeader(data: SparkRDD[String]): SparkRDD[String] = {
    data.mapPartitionsWithIndex((idx, lines) => {
      if (idx == 0) {
        lines.drop(1)
      }
      lines
    })
  }

  def isNumeric(input: String): Boolean = input.forall(_.isDigit)

  def mappingData(data: SparkRDD[String]): SparkRDD[Rating] = {
    //ratings.data of MovieLens
    val header = data.first()
    akkaLogger.warn("Mapping...", header)

    data.filter(_ != header).flatMap(_.split(",") match {
      /*
    case Array(uniqueId, gender, gameId, theme, style, community, type1, type2, mobile, saving, revenue) => {
      val gameIdNoQuotes = gameId.replace("\"", "")
      Some(Rating(uniqueId.toInt, gameIdNoQuotes.toInt, revenue.toDouble))
    }
    */
      case Array(pub_id, gender_mf, gender, game_id, theme, style, community, type1, type2, web_mobile, login_days, login_times, duration_sec, pay_times, saving, saving02) => {
        val gameIdNoQuotes = game_id.replace("\"", "")
        Some(Rating(pub_id.toInt, gameIdNoQuotes.toInt, saving.toDouble))
      }
      case some =>
        akkaLogger.warn("data error:" + some.mkString(","))
        None
    })
  }

  def ExecAls(sc: SparkContext) = {

    // Load and parse the data
    akkaLogger.warn("Load into RDD...")
    val trainingData: SparkRDD[String] = sc.textFile(TRAINING_DATA_IN_PATH)
    val testData: SparkRDD[String] = sc.textFile(TEST_DATA_IN_PATH)
    val ratings: SparkRDD[Rating] = mappingData(trainingData)
    val ratingsTest: SparkRDD[Rating] = mappingData(testData)
    akkaLogger.warn("Training Data Size=" + ratings.count)
    akkaLogger.warn("Test Data Size=" + ratingsTest.count)

    // Build the recommendation model using ALS
    val rank = 10 //number of lantent factors
    val numIterations = 20
    val lambda = 0.01 //normalization parameter
    akkaLogger.warn("Training...")
    val model = ALS.train(ratings, rank, numIterations, lambda)

    // Evaluate the model on rating data
    val usersProducts = ratingsTest.map { case Rating(user, product, rate) =>
      (user, product)
    }

    akkaLogger.warn("Predicting...")
    val predictions =
      model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }

    akkaLogger.warn("Joining...")
    val ratesAndPreds = ratingsTest.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions).sortByKey() //ascending or descending

    akkaLogger.warn("Try to delete path: [" + OUTPUT_HADOOP_PATH + "]")
    val delete_out_path = "hadoop fs -rm -r -f " + OUTPUT_HADOOP_PATH
    delete_out_path.!

    val formatedRatesAndPreds = ratesAndPreds.map {
      case ((user, product), (rate, pred)) =>
        user + "\t" + product + "\t" + rate + "\t" + "%02.4f" format pred
    }

    formatedRatesAndPreds.saveAsTextFile(OUTPUT_HADOOP_PATH)

    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      akkaLogger.warn("output=" + user + "\t" + product + "\t" + r1 + "\t" + "%02.4f" format r2)
      val err = (r1 - r2)
      err * err
    }.mean()

    akkaLogger.warn("--->Mean Squared Error = " + MSE)
    akkaLogger.warn(calConfusionMatrix(ratesAndPreds).toString)
  }

  case class ConfusionMatrixResult(accuracy: Double, precision: Double, recall: Double, fallout: Double, sensitivity: Double, specificity: Double, f: Double) {

    override def toString: String = {
      s"\n" +
        s"Accuracy=$accuracy\n" +
        s"Precision=$precision\n" +
        s"Recall=$recall\n" +
        s"Fallout=$fallout\n" +
        s"Sensitivity=$sensitivity\n" +
        s"Specificity=$specificity\n" +
        s"F=$f"
    }
  }

  case class ConfusionMatrix(tp: Double, fp: Double, fn: Double, tn: Double)

  def calConfusionMatrix(data: SparkRDD[((Int, Int), (Double, Double))]): ConfusionMatrixResult = {

    val confusionMatrix = data.map {
      case ((user, product), (r1, r2)) =>
        val pred = if (r2 > 0) 1 else 0
        r1 match {
          case revenue if revenue > 0 =>
            if (pred == 1) ConfusionMatrix(1, 0, 0, 0)
            else ConfusionMatrix(0, 0, 1, 0)
          case revenue if revenue <= 0 =>
            if (pred == 1) ConfusionMatrix(0, 1, 0, 0)
            else ConfusionMatrix(0, 0, 0, 1)
        }
    }

    val result = confusionMatrix.reduce((sum, row) ⇒ ConfusionMatrix(sum.tp + row.tp, sum.fp + row.fp, sum.fn + row.fn, sum.tn + row.tn))
    val p = result.tp + result.fn
    val n = result.fp + result.tn
    val accuracy = (result.tp + result.tn) / (p + n)
    val precision = result.tp / (result.tp + result.fp)
    val recall = result.tp / p
    val fallout = result.fp / n
    val sensitivity = result.tp / (result.tp + result.fn)
    val specificity = result.tn / (result.fp + result.tn)
    val f = 2 * ((precision * recall) / (precision + recall))
    ConfusionMatrixResult(accuracy, precision, recall, fallout, sensitivity, specificity, f)
  }
}