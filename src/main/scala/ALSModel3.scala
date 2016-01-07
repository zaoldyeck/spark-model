import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD

import scala.collection.immutable.IndexedSeq
import scala.compat.Platform
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.sys.process._
import scala.util.{Random, Try}

/**
  * Created by zaoldyeck on 2016/1/6.
  */
class ALSModel3 extends ALSModel {
  private val TRAINING_DATA_PATH: String = "hdfs://pubgame/user/vincent/pg_user_game_90_training_v3.csv"
  private val PREDICTION_DATA_PATH: String = "hdfs://pubgame/user/vincent/pg_user_game_90_other.csv"
  private val OUTPUT_PATH: String = "hdfs://pubgame/user/vincent/spark-als"

  case class PredictResult(user: Int, product: Int, predict: Double, fact: Double)

  override def run(implicit sc: SparkContext): Unit = {
    val trainingData: RDD[Rating] = mappingData(sc.textFile(TRAINING_DATA_PATH)).persist
    val predictionData: RDD[Rating] = mappingData(sc.textFile(PREDICTION_DATA_PATH)).persist
    val fileSystem: FileSystem = FileSystem.get(new Configuration)
    //val delete_out_path: String = "hadoop fs -rm -f -r " + OUTPUT_PATH
    //delete_out_path.!

    case class AlsParameters(rank: Int = 10, lambda: Double = 0.01, alpha: Double = 0.01)
    val parametersSeq: IndexedSeq[AlsParameters] = for {
      rank <- 2 until 50 by 2
      lambda <- 0.0001 until 15 by 0.1
      alpha <- 0.0001 until 50 by 0.1
    } yield new AlsParameters(rank, lambda, alpha)

    Random.shuffle(parametersSeq).foreach(parameters => Await.result({
      case class Prediction(_1: RDD[Rating], _2: RDD[Rating], _3: RDD[Rating], _4: RDD[Rating])
      val split: Prediction = predictionData.randomSplit(Array.fill(4)(0.25), Platform.currentTime) match {
        case Array(split_1, split_2, split_3, split_4) => Prediction(split_1, split_2, split_3, split_4)
      }

      case class Evaluation(output: String, recall: Double) {
        override def toString: String = output
      }
      def evaluateModel(trainingData: RDD[Rating], testingData: RDD[Rating]): Future[Evaluation] = Future {
        Logger.log.warn("Evaluate")
        val predictResult: RDD[PredictResult] = ALS.trainImplicit(trainingData, parameters.rank, 50, parameters.lambda, parameters.alpha)
          .predict(testingData.map(dataSet => (dataSet.user, dataSet.product)))
          .map(predict => ((predict.user, predict.product), predict.rating))
          .join(testingData.map(dataSet => ((dataSet.user, dataSet.product), dataSet.rating))) map {
          case ((user, product), (predict, fact)) => PredictResult(user, product, predict, fact)
        }
        val evaluation: ConfusionMatrixResult = calConfusionMatrix(predictResult)
        val output: String = evaluation.toListString
        Logger.log.warn(output)
        Evaluation(output, evaluation.recall)
      }

      val evaluateModel_1: Future[Evaluation] = evaluateModel(trainingData union split._2 union split._3 union split._4, split._1)
      val evaluateModel_2: Future[Evaluation] = evaluateModel(trainingData union split._1 union split._3 union split._4, split._2)
      val evaluateModel_3: Future[Evaluation] = evaluateModel(trainingData union split._1 union split._2 union split._4, split._3)
      val evaluateModel_4: Future[Evaluation] = evaluateModel(trainingData union split._1 union split._2 union split._3, split._4)

      for {
        evaluation_1: Evaluation <- evaluateModel_1
        evaluation_2: Evaluation <- evaluateModel_2
        evaluation_3: Evaluation <- evaluateModel_3
        evaluation_4: Evaluation <- evaluateModel_4
      } yield {
        val printWriter: PrintWriter = new PrintWriter(fileSystem.create(new Path(s"$OUTPUT_PATH/${System.nanoTime}")))
        try {
          val recalls: List[Double] = List(evaluation_1.recall, evaluation_2.recall, evaluation_3.recall, evaluation_4.recall)
          printWriter.write(s"rank:${parameters.rank},lambda:${parameters.lambda},alpha:${parameters.alpha}\n" +
            s"$evaluation_1\n$evaluation_2\n$evaluation_3\n$evaluation_4\n" +
            s"Average:${"%.4f".format(recalls.sum / recalls.length)}\n" +
            s"Difference:${"%.4f".format(recalls.max - recalls.min)}\n" +
            s"--------------------------------------------------------------------------------------------------------\n")
        } finally {
          printWriter.close()
        }
      }
    }, Duration.Inf))
  }

  def calConfusionMatrix(predictResult: => RDD[PredictResult]): ConfusionMatrixResult = {
    val result: ConfusionMatrix = predictResult.map {
      case result: PredictResult if result.fact > 0 && result.predict > 0 => ConfusionMatrix(tp = 1)
      case result: PredictResult if result.fact > 0 && result.predict <= 0 => ConfusionMatrix(fn = 1)
      case result: PredictResult if result.fact <= 0 && result.predict > 0 => ConfusionMatrix(fp = 1)
      case _ ⇒ ConfusionMatrix(tn = 1)
    }.reduce((sum, row) => ConfusionMatrix(sum.tp + row.tp, sum.fp + row.fp, sum.fn + row.fn, sum.tn + row.tn))

    val p: Double = result.tp + result.fn
    val n: Double = result.fp + result.tn
    val accuracy: Double = (result.tp + result.tn) / (p + n)
    val precision: Double = result.tp / (result.tp + result.fp)
    val recall: Double = result.tp / p
    val fallout: Double = result.fp / n
    val sensitivity: Double = result.tp / (result.tp + result.fn)
    val specificity: Double = result.tn / (result.fp + result.tn)
    val f: Double = 2 * ((precision * recall) / (precision + recall))
    ConfusionMatrixResult(accuracy, precision, recall, fallout, sensitivity, specificity, f)
  }
}