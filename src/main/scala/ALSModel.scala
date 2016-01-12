import org.apache.spark._
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD

import scala.sys.process._

/**
  * Created by zaoldyeck on 2015/12/23.
  */
class ALSModel extends Serializable {
  private val TRAINING_DATA_IN_PATH = "s3n://data.emr/pg_user_game_90_training_v3.csv"
  private val TEST_DATA_IN_PATH = "s3n://data.emr/pg_user_game_90_other.csv"
  //private val TRAINING_DATA_IN_PATH = "hdfs://pubgame/user/cray/SparkAls/pg_user_game_90_training_web.txt"
  //private val TEST_DATA_IN_PATH = "hdfs://pubgame/user/cray/SparkAls/pg_user_game_90_test_01.txt"
  //private val TRAINING_DATA_IN_PATH = "hdfs://pubgame/user/vincent/pg_user_game_90_training_v2.csv"
  //private val TEST_DATA_IN_PATH = "hdfs://pubgame/user/vincent/pg_user_game_90_test_v2.csv"
  //private val TRAINING_DATA_IN_PATH = "hdfs://pubgame/user/vincent/pg_user_game_78_training_web.csv"
  //private val TEST_DATA_IN_PATH = "hdfs://pubgame/user/vincent/pg_user_game_78_test.csv"
  private val OUTPUT_PATH = "./spark-als"

  def run(sc: SparkContext): Unit = {

    // Load and parse the data
    Logger.log.warn("Load into RDD...")
    val trainingData: RDD[String] = sc.textFile(TRAINING_DATA_IN_PATH)
    val testData: RDD[String] = sc.textFile(TEST_DATA_IN_PATH)
    //val ratings: SparkRDD[Rating] = ratingData(mappingData(trainingData))

    val ratings: RDD[Rating] = mappingData(trainingData)
    val ratingsTest: RDD[Rating] = mappingData(testData)
    Logger.log.warn("Training Data Size=" + ratings.count)
    Logger.log.warn("Test Data Size=" + ratingsTest.count)

    // Build the recommendation model using ALS
    val rank = 10 //number of latent factors
    val numIterations = 10
    val lambda = 0.01 //normalization parameter
    val alpha = 0.01

    Logger.log.warn("Training...")
    val model = ALS.trainImplicit(ratings, rank, numIterations, lambda, alpha)
    //val model = ALS.train(ratings, rank, numIterations, lambda)

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

  val dropHeader = (data: RDD[String]) => {
    data.mapPartitionsWithIndex {
      case (0, lines) if lines.hasNext =>
        lines.next
        lines
      case (_, lines) => lines
    }
  }

  val mappingData = (data: RDD[String]) => {
    Logger.log.warn("Mapping...")

    dropHeader(data) flatMap {
      _.split(",") match {
        case Array(pub_id, game_id, saving) =>
          val gameIdNoQuotes = game_id.replace("\"", "")
          val rating = saving.toDouble
          Some(Rating(pub_id.toInt, gameIdNoQuotes.toInt, if (rating > 0) 1 else 0))
        case some =>
          Logger.log.warn("data error:" + some.mkString(","))
          None
      }
    }
  }

  private def ratingData(data: RDD[Rating]): RDD[Rating] = data map {
    case Rating(user, product, rating) => Rating(user, product, if (rating > 0) 1 else 0)
  }

  case class ConfusionMatrix(tp: Double = 0, fp: Double = 0, fn: Double = 0, tn: Double = 0)

  case class ConfusionMatrixResult(accuracy: Double, precision: Double, recall: Double, fallout: Double, sensitivity: Double, specificity: Double, f: Double) {
    override def toString: String = {
      s"\n" +
        s"Accuracy = $accuracy\n" +
        s"Precision = $precision\n" +
        s"Recall = $recall\n" +
        s"Fallout = $fallout\n" +
        s"Sensitivity = $sensitivity\n" +
        s"Specificity = $specificity\n" +
        s"F = $f"
    }

    /*
    val toListString: String = {
      s"${"%.4f".format(accuracy)}," +
        s"${"%.4f".format(precision)}," +
        s"${"%.4f".format(recall)}," +
        s"${"%.4f".format(fallout)}," +
        s"${"%.4f".format(sensitivity)}," +
        s"${"%.4f".format(specificity)}," +
        s"${"%.4f".format(f)}"
    }
    */
  }

  val calConfusionMatrix = (data: RDD[((Int, Int), (Double, Double))]) => {
    val result = data.map {
      case ((user, product), (fact, pred)) if fact > 0 && pred > 0 => ConfusionMatrix(tp = 1)
      case ((user, product), (fact, pred)) if fact > 0 && pred <= 0 => ConfusionMatrix(fn = 1)
      case ((user, product), (fact, pred)) if fact <= 0 && pred > 0 => ConfusionMatrix(fp = 1)
      case _ ⇒ ConfusionMatrix(tn = 1)
    }.reduce((sum, row) => ConfusionMatrix(sum.tp + row.tp, sum.fp + row.fp, sum.fn + row.fn, sum.tn + row.tn))

    val p = result.tp + result.fn
    val n = result.fp + result.tn
    Logger.log.warn("P=" + p)
    Logger.log.warn("N=" + n)

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
