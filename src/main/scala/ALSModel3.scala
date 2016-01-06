import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD

import scala.collection.immutable.IndexedSeq
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.sys.process._
import scala.util.{Random, Try}

/**
  * Created by zaoldyeck on 2016/1/6.
  */
class ALSModel3 extends ALSModel {
  private val TRAINING_DATA_PATH = "hdfs://pubgame/user/vincent/pg_user_game_90_training_v3.csv"
  private val PREDICTION_DATA_PATH = "hdfs://pubgame/user/vincent/pg_user_game_90_other.csv"
  private val OUTPUT_PATH = "hdfs://pubgame/user/vincent/spark-als"

  case class PredictResult(user: Int, product: Int, predict: Double, fact: Double)

  override def run(implicit sc: SparkContext): Unit = {
    val trainingData: RDD[Rating] = mappingData(sc.textFile(TRAINING_DATA_PATH)).persist
    val predictionData: Array[Rating] = mappingData(sc.textFile(PREDICTION_DATA_PATH)).persist.collect
    val fileSystem: FileSystem = FileSystem.get(new Configuration)
    val length: Int = predictionData.length
    val delete_out_path = "hadoop fs -rm -f -r " + OUTPUT_PATH
    delete_out_path.!

    case class AlsParameters(rank: Int = 10, lambda: Double = 0.01, alpha: Double = 0.01)
    val parametersSeq: IndexedSeq[AlsParameters] = for {
      rank <- 2 until 50 by 2
      lambda <- 0.0001 until 15 by 0.1
      alpha <- 0.0001 until 50 by 0.1
    } yield new AlsParameters(rank, lambda, alpha)

    Await.result(Future.sequence(Random.shuffle(parametersSeq).map(parameters => Future {
      case class Prediction(_1: RDD[Rating], _2: RDD[Rating], _3: RDD[Rating], _4: RDD[Rating])
      val split: Prediction = Random.shuffle(predictionData.toSeq).splitAt(length / 2) match {
        case (half1, half2) =>
          val half1Split: (Seq[Rating], Seq[Rating]) = half1.splitAt(half1.length / 2)
          val half2Split: (Seq[Rating], Seq[Rating]) = half2.splitAt(half2.length / 2)
          Prediction(
            sc.parallelize(half1Split._1),
            sc.parallelize(half1Split._2),
            sc.parallelize(half2Split._1),
            sc.parallelize(half2Split._2))
      }

      val output_1: String = evaluateModel(trainingData union split._2 union split._3 union split._4, split._1)
      val output_2: String = evaluateModel(trainingData union split._1 union split._3 union split._4, split._2)
      val output_3: String = evaluateModel(trainingData union split._1 union split._2 union split._4, split._3)
      val output_4: String = evaluateModel(trainingData union split._1 union split._2 union split._3, split._4)
      val printWriter: PrintWriter = new PrintWriter(fileSystem.create(new Path(s"$OUTPUT_PATH/${System.nanoTime}")))
      Try {
        printWriter.write(s"$output_1\n$output_2\n$output_3\n$output_4\n-------------------------------\n")
      } match {
        case _ => printWriter.close()
      }

      def evaluateModel(trainingData: RDD[Rating], testingData: RDD[Rating]): String = {
        val rank: Int = 10
        val numIterations: Int = 10
        val lambda: Double = 0.01
        val alpha: Double = 0.01

        val predictResult: RDD[PredictResult] = ALS.trainImplicit(trainingData, rank, numIterations, lambda, alpha)
          .predict(testingData.map(dataSet => (dataSet.user, dataSet.product)))
          .map(predict => ((predict.user, predict.product), predict.rating))
          .join(testingData.map(dataSet => ((dataSet.user, dataSet.product), dataSet.rating))) map {
          case ((user, product), (predict, fact)) => PredictResult(user, product, predict, fact)
        }
        s"rank:$rank,lambda:$lambda,alpha:$alpha,${calConfusionMatrix(predictResult).toListString}"
      }
    })), Duration.Inf)
  }

  def calConfusionMatrix(predictResult: => RDD[PredictResult]): ConfusionMatrixResult = {
    val confusionMatrix = predictResult.map {
      case result: PredictResult if result.fact > 0 && result.predict > 0 => ConfusionMatrix(tp = 1)
      case result: PredictResult if result.fact > 0 && result.predict <= 0 => ConfusionMatrix(fn = 1)
      case result: PredictResult if result.fact <= 0 && result.predict > 0 => ConfusionMatrix(fp = 1)
      case _ ⇒ ConfusionMatrix(tn = 1)
    }

    val result = confusionMatrix.reduce((sum, row) => ConfusionMatrix(sum.tp + row.tp, sum.fp + row.fp, sum.fn + row.fn, sum.tn + row.tn))
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