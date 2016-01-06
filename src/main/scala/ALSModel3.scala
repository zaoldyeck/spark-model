import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, ALS, Rating}
import org.apache.spark.rdd.RDD
import scala.util.Random

/**
  * Created by zaoldyeck on 2016/1/6.
  */
class ALSModel3 extends ALSModel {
  private val TRAINING_DATA_PATH = "hdfs://pubgame/user/vincent/pg_user_game_78_training_web.csv"
  private val PREDICTION_DATA_PATH = "hdfs://pubgame/user/vincent/pg_user_game_78_training_web.csv"
  private val OUTPUT_PATH = "hdfs://pubgame/user/vincent/spark-als"

  case class PredictResult(user: Int, product: Int, predict: Double, fact: Double)

  override def run(implicit sc: SparkContext): Unit = {
    val trainingData: RDD[Rating] = mappingData(sc.textFile(TRAINING_DATA_PATH)).persist
    val predictionData: Array[Rating] = mappingData(sc.textFile(PREDICTION_DATA_PATH)).persist.collect

    val length: Int = predictionData.length
    case class Prediction(testing: Array[Rating], training: Array[Rating])
    val split: Prediction = Random.shuffle(predictionData).splitAt(length / 4) match {
      case (testing, training) => Prediction(testing, training)
    }
    val trainingDataSet: RDD[Rating] = sc.parallelize(split.testing)
    val testingDataSet: RDD[Rating] = sc.parallelize(split.training)

    val rank = 10
    val numIterations = 10
    val lambda = 0.01
    val alpha = 0.01

    val predictResult: RDD[PredictResult] = ALS.trainImplicit(trainingData union trainingDataSet, rank, numIterations, lambda, alpha)
      .predict(testingDataSet.map(dataSet => (dataSet.user, dataSet.product)))
      .map(predict => ((predict.user, predict.product), predict.rating))
      .join(testingDataSet.map(dataSet => ((dataSet.user, dataSet.product), dataSet.rating))) map {
      case ((user, product), (predict, fact)) => PredictResult(user, product, predict, fact)
    }

    val mse: Double = predictResult.map(result => Math.pow(result.predict - result.fact, 2)).mean

    predictResult.map(result => result.user + "\t" + result.product + "\t" + result.fact + "\t" + "%02.4f" format result.predict)
      .saveAsTextFile(OUTPUT_PATH)

    Logger.log.warn("--->Mean Squared Error = " + mse)
    Logger.log.warn(calConfusionMatrix(predictResult).toString)
  }

  def calConfusionMatrix(predictResult: RDD[PredictResult]): ConfusionMatrixResult = {
    val confusionMatrix = predictResult.map {
      case result: PredictResult if result.fact > 0 && result.predict > 0 => ConfusionMatrix(tp = 1)
      case result: PredictResult if result.fact > 0 && result.predict <= 0 => ConfusionMatrix(fn = 1)
      case result: PredictResult if result.fact <= 0 && result.predict > 0 => ConfusionMatrix(fp = 1)
      case _ ⇒ ConfusionMatrix(tn = 1)
    }

    val result = confusionMatrix.reduce((sum, row) => ConfusionMatrix(sum.tp + row.tp, sum.fp + row.fp, sum.fn + row.fn, sum.tn + row.tn))
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