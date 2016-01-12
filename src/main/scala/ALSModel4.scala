import java.io.{FileOutputStream, PrintWriter}

import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD

/**
  * Created by zaoldyeck on 2016/1/12.
  */
class ALSModel4 extends Serializable {

  private val DataPath: String = "s3n://data.emr/not_only_90.csv"
  private val OutputPath: String = "/home/hadoop/output/not-only-90.txt"

  def run(implicit sc: SparkContext): Unit = {
    val data: Array[Rating] = mappingData(sc.textFile(DataPath)).collect
    Logger.log.warn("Total Size:" + data.length)
    val rddOnly90: Array[Rating] = data.filter(_.product == 90)
    Logger.log.warn("90 Size:" + rddOnly90.length)
    rddOnly90.foreach(prediction => {
      Logger.log.warn("Predict...")
      val trainingRDD: RDD[Rating] = sc.parallelize(data.filter(rating => rating.user != prediction.user && rating.product != prediction.product))
      val result: Double = ALS.trainImplicit(trainingRDD, 10, 10, 0.01, 0.01).predict(prediction.user, prediction.product)
      Logger.log.warn("Result:" + prediction.rating + "," + result)
      val printWriter: PrintWriter = new PrintWriter(new FileOutputStream(s"$OutputPath"))
      try {
        printWriter.write(prediction.rating + "," + result.toString)
      } catch {
        case e: Exception => Logger.log.error(e.printStackTrace())
      } finally printWriter.close()
    })
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
        case Array(pub_id, game_id, saving, play_game_count, is_play_90) =>
          val gameIdNoQuotes = game_id.replace("\"", "")
          val rating = saving.toDouble
          Some(Rating(pub_id.toInt, gameIdNoQuotes.toInt, if (rating > 0) 1 else 0))
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

  case class PredictResult(user: Int, product: Int, predict: Double, fact: Double)

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