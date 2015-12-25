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
  private val TRAINING_DATA_IN_PATH = "hdfs://pubgame/user/vincent/pg_with_gd_for_als_training.csv"
  private val TEST_DATA_IN_PATH = "hdfs://pubgame/user/vincent/pg_with_gd_for_als_testing_inner.csv"

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
      case Array(uniqueId, gameId, saving) => {
        val gameIdNoQuotes = gameId.replace("\"", "")
        Some(Rating(uniqueId.toInt, gameIdNoQuotes.toInt, saving.toDouble))
      }
      case some => None
    })
  }

  def ExecAls(sc: SparkContext) = {

    // Load and parse the data
    akkaLogger.warn("Load into RDD...")
    val trainingData: SparkRDD[String] = sc.textFile(TRAINING_DATA_IN_PATH)
    val testData: SparkRDD[String] = sc.textFile(TEST_DATA_IN_PATH)


    //val noheader_data = data.zipWithIndex().filter(_._2 > 0)
    //val noheader_data = dropHeader(data)

    val ratings: SparkRDD[Rating] = mappingData(trainingData)
    val ratings_test: SparkRDD[Rating] = mappingData(testData)

    System.out.flush()

    // Build the recommendation model using ALS
    val rank = 10 //number of lantent factors
    val numIterations = 20
    val lambda = 0.01 //normalization parameter
    akkaLogger.warn("Training...")
    val model = ALS.train(ratings, rank, numIterations, lambda)

    // Evaluate the model on rating data
    val usersProducts = ratings_test.map { case Rating(user, product, rate) =>
      (user, product)
    }

    akkaLogger.warn("Predicting...")
    val predictions =
      model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }

    akkaLogger.warn("Joining...")
    val ratesAndPreds = ratings_test.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions).sortByKey() //ascending or descending


    akkaLogger.warn("Try to delete path: [" + OUTPUT_HADOOP_PATH + "]")
    val delete_out_path = "hadoop fs -rm -r -f " + OUTPUT_HADOOP_PATH
    delete_out_path.!

    val formatedRatesAndPreds = ratesAndPreds.map {
      case ((user, product), (rate, pred)) => user + "\t" + product + "\t" + rate + "\t" + "%02.4f" format pred
    }
    formatedRatesAndPreds.saveAsTextFile(OUTPUT_HADOOP_PATH)

    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()

    akkaLogger.warn("--->Mean Squared Error = " + MSE)
    akkaLogger.warn(calConfusionMatrix(ratesAndPreds).toString)
  }

  case class ConfusionMatrixResult(accuracy: Double, precision: Double, recall: Double, fallout: Double, sensitivity: Double, specificity: Double, f: Double)

  case class ConfusionMatrix(tp: Double, fp: Double, fn: Double, tn: Double)

  def calConfusionMatrix(data: SparkRDD[((Int, Int), (Double, Double))]): ConfusionMatrixResult = {

    val confusionMatrix = data.map {
      case ((user, product), (r1, r2)) ⇒
        val pred = if (r2 >= 0.5) 1 else 0
        r1 match {
          case 1 ⇒
            if (pred == 1) ConfusionMatrix(1, 0, 0, 0)
            else ConfusionMatrix(0, 0, 1, 0)
          case 0 ⇒
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