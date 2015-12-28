import akka.event.slf4j.Logger
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark._
import org.apache.spark.rdd.{RDD => SparkRDD}
import org.slf4j.LoggerFactory
import sys.process._

class ALSModel {
  val sparkLogger = LoggerFactory.getLogger(getClass)
  val akkaLogger = Logger("！！This Is Important Message！！")

  private val OUTPUT_HADOOP_PATH = "hdfs://pubgame/user/vincent/spark-als"
  //private val TRAINING_DATA_IN_PATH = "hdfs://pubgame/user/vincent/pg_with_gd_for_model_with_revenue_training.csv"
  //private val TEST_DATA_IN_PATH = "hdfs://pubgame/user/vincent/pg_with_gd_for_model_with_revenue_testing_inner.csv"
  private val TRAINING_DATA_IN_PATH = "hdfs://pubgame/user/vincent/pg_user_game_90_training.csv"
  private val TEST_DATA_IN_PATH = "hdfs://pubgame/user/vincent/pg_user_game_90_test.csv"

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
      case Array(pub_id, game_id, saving) =>
        val gameIdNoQuotes = game_id.replace("\"", "")
        Some(Rating(pub_id.toInt, gameIdNoQuotes.toInt, saving.toDouble))
      case some =>
        akkaLogger.warn("data error:" + some.mkString(","))
        None
    })
  }

  def ratingData(data: SparkRDD[Rating]): SparkRDD[Rating] = {
    val sortedData = data.sortBy(_.rating)

    val dataNotSavingSize = sortedData.filter(_.rating <= 0).count
    val dataHasSavingSize = sortedData.filter(_.rating > 0).count

    sortedData.zipWithIndex.map {
      case (rating, index) =>
        index match {
          case i if i < dataNotSavingSize => Rating(rating.user, rating.product, -1)
          case i if i < dataNotSavingSize + dataHasSavingSize / 10 => Rating(rating.user, rating.product, 1)
          case i if i < dataNotSavingSize + dataHasSavingSize / 10 * 2 => Rating(rating.user, rating.product, 2)
          case i if i < dataNotSavingSize + dataHasSavingSize / 10 * 3 => Rating(rating.user, rating.product, 3)
          case i if i < dataNotSavingSize + dataHasSavingSize / 10 * 4 => Rating(rating.user, rating.product, 4)
          case i if i < dataNotSavingSize + dataHasSavingSize / 10 * 5 => Rating(rating.user, rating.product, 5)
          case i if i < dataNotSavingSize + dataHasSavingSize / 10 * 6 => Rating(rating.user, rating.product, 6)
          case i if i < dataNotSavingSize + dataHasSavingSize / 10 * 7 => Rating(rating.user, rating.product, 7)
          case i if i < dataNotSavingSize + dataHasSavingSize / 10 * 8 => Rating(rating.user, rating.product, 8)
          case i if i < dataNotSavingSize + dataHasSavingSize / 10 * 9 => Rating(rating.user, rating.product, 9)
          case i if i < dataNotSavingSize + dataHasSavingSize => Rating(rating.user, rating.product, 10)
        }
    }
  }

  def run(sc: SparkContext) = {

    // Load and parse the data
    akkaLogger.warn("Load into RDD...")
    val trainingData: SparkRDD[String] = sc.textFile(TRAINING_DATA_IN_PATH)
    val testData: SparkRDD[String] = sc.textFile(TEST_DATA_IN_PATH)
    val ratings: SparkRDD[Rating] = ratingData(mappingData(trainingData))
    val ratingsTest: SparkRDD[Rating] = mappingData(testData)
    akkaLogger.warn("Training Data Size=" + ratings.count)
    akkaLogger.warn("Test Data Size=" + ratingsTest.count)

    // Build the recommendation model using ALS
    val rank = 10 //number of lantent factors
    val numIterations = 100
    val lambda = 0.01 //normalization parameter
    akkaLogger.warn("Training...")
    val model = ALS.train(ratings, rank, numIterations, lambda)

    // Evaluate the model on rating data
    val usersProducts = ratingsTest.map { case Rating(user, product, rate) =>
      (user, product)
    }

    akkaLogger.warn("Predicting...")
    val predictions = model.predict(usersProducts).map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }
    akkaLogger.warn("Predictions Size=" + predictions.count)

    akkaLogger.warn("Joining...")
    val ratesAndPreds = ratingsTest.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions).sortByKey() //ascending or descending

    akkaLogger.warn("Try to delete path: [" + OUTPUT_HADOOP_PATH + "]")
    val delete_out_path = "hadoop fs -rm -f -r " + OUTPUT_HADOOP_PATH
    delete_out_path.!

    val formatedRatesAndPreds = ratesAndPreds.map {
      case ((user, product), (rate, pred)) =>
        val output = user + "\t" + product + "\t" + rate + "\t" + "%02.4f" format pred
        akkaLogger.warn("output=" + output)
        output
    }

    formatedRatesAndPreds.saveAsTextFile(OUTPUT_HADOOP_PATH)

    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
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

  case class ConfusionMatrix(tp: Double = 0, fp: Double = 0, fn: Double = 0, tn: Double = 0)

  def calConfusionMatrix(data: SparkRDD[((Int, Int), (Double, Double))]): ConfusionMatrixResult = {
    val confusionMatrix = data.map {
      case ((user, product), (fact, pred)) if fact > 0 && pred > 0 ⇒
        ConfusionMatrix(tp = 1)
      case ((user, product), (fact, pred)) if fact > 0 && pred <= 0 ⇒
        ConfusionMatrix(fn = 1)
      case ((user, product), (fact, pred)) if fact <= 0 && pred > 0 ⇒
        ConfusionMatrix(fp = 1)
      case _ ⇒
        ConfusionMatrix(tn = 1)
    }

    val result = confusionMatrix.reduce((sum, row) ⇒ ConfusionMatrix(sum.tp + row.tp, sum.fp + row.fp, sum.fn + row.fn, sum.tn + row.tn))
    val p = result.tp + result.fn
    val n = result.fp + result.tn
    akkaLogger.warn("P=" + p)
    akkaLogger.warn("N=" + n)
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