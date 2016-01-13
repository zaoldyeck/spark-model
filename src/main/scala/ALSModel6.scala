import java.io.{FileOutputStream, PrintWriter}

import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD

import scala.compat.Platform
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Random

/**
  * Created by zaoldyeck on 2016/1/12.
  */
class ALSModel6 extends Serializable {

  private val DataPath: String = "s3n://data.emr/als_web_saving.csv"
  private val OutputPath: String = "/home/hadoop/output/all.txt"

  def run(implicit sc: SparkContext): Unit = {
    val all: RDD[Rating] = mappingData(sc.textFile(DataPath)).persist
    Logger.log.warn("Total Size:" + all.count)

    case class AlsParameters(rank: Int = 10, lambda: Double = 0.01, alpha: Double = 0.01)

    val parametersSeq: IndexedSeq[AlsParameters] = for {
      rank <- 2 until 50 by 1
      lambda <- 0.1 until 15 by 0.1
      alpha <- 0.1 until 50 by 0.1
    } yield new AlsParameters(rank, lambda, alpha)

    val futures: IndexedSeq[Future[Any]] = Random.shuffle(parametersSeq).zipWithIndex map {
      case (parameters, index) => Future {
        all.randomSplit(Array(0.75, 0.25), Platform.currentTime) match {
          case Array(training, prediction) =>
            Logger.log.warn("Training Size:" + training.count)
            Logger.log.warn("Predicting Size:" + prediction.count)
            Logger.log.warn("Predict...")
            val predictResult: RDD[PredictResult] = ALS.trainImplicit(training, 20, parameters.rank, parameters.lambda, parameters.alpha)
              .predict(prediction.map(rating => (rating.user, rating.product)))
              .map(predict => ((predict.user, predict.product), predict.rating))
              .join(prediction.map(result => ((result.user, result.product), result.rating))) map {
              case ((user, product), (predict, fact)) => PredictResult(user, product, predict, fact)
            }
            predictResult.saveAsTextFile("./als-all")
            val header: String = s"$index,${parameters.rank},${parameters.lambda},${parameters.alpha}"
            val result: ConfusionMatrixResult = calConfusionMatrix(predictResult)
            Logger.log.warn(result.toString)
            val printWriter: PrintWriter = new PrintWriter(new FileOutputStream(s"$OutputPath", true))
            try {
              printWriter.append(header + "," + result.toListString)
              printWriter.println()
            } catch {
              case e: Exception => Logger.log.error(e.printStackTrace())
            } finally printWriter.close()
        }
      }
    }
    Await.result(Future.sequence(futures), Duration.Inf)
  }

  def dropHeader(data: RDD[String]): RDD[String] = {
    data.mapPartitionsWithIndex {
      case (0, lines) if lines.hasNext =>
        lines.next
        lines
      case (_, lines) => lines
    }
  }

  def mappingData(data: RDD[String]): RDD[Rating] = {
    Logger.log.warn("Mapping...")

    dropHeader(data) flatMap {
      _.split(",") match {
        case Array(pub_id, game_id, saving) =>
          val gameIdNoQuotes = game_id.replace("\"", "")
          val rating = saving.toDouble
          Some(Rating(pub_id.toInt, gameIdNoQuotes.toInt, rating))
        case some =>
          Logger.log.warn("data error:" + some.mkString(","))
          None
      }
    }
  }

  def calConfusionMatrix(predictResult: RDD[PredictResult]): ConfusionMatrixResult = {
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

  case class PredictResult(user: Int, product: Int, predict: Double, fact: Double) {
    override def toString: String = s"$user,$product,$fact,$predict"
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

    def toListString: String = {
      s"${"%.4f".format(accuracy)}," +
        s"${"%.4f".format(precision)}," +
        s"${"%.4f".format(recall)}," +
        s"${"%.4f".format(fallout)}," +
        s"${"%.4f".format(sensitivity)}," +
        s"${"%.4f".format(specificity)}," +
        s"${"%.4f".format(f)}"
    }
  }

}