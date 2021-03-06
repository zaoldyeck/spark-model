import java.io.{FileOutputStream, PrintWriter}
import java.util.concurrent.Semaphore

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.immutable
import scala.collection.immutable.IndexedSeq
import scala.compat.Platform
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.sys.process._
import scala.util.Random

/**
  * Created by zaoldyeck on 2016/1/6.
  */
class ALSFold extends ALSSample {

  case class DataSet(trainingData: RDD[Rating], predictionData: RDD[Rating], outputPath: String)

  case class AlsParameters(rank: Int = 10, iterations: Int = 40, lambda: Double = 0.01, alpha: Double = 0.01)

  case class Evaluation(predictResult: RDD[PredictResult], output: String, recall: Double) {
    override def toString: String = output
  }

  case class PredictResult(user: Int, product: Int, predict: Double, fact: Double)

  override def run(implicit sc: SparkContext): Unit = {

    def DataSet_(trainingDataPath: String, predictionDataPath: String, outputPath: String): DataSet = {
      DataSet(
        mappingData(sc.textFile(trainingDataPath)).persist,
        mappingData(sc.textFile(predictionDataPath)).persist,
        outputPath)
    }

    lazy val sqlContext: SQLContext = new SQLContext(sc)

    object DataFrame_ {
      def apply(trainingDataPath: String, predictionDataPath: String, outputPath: String): DataSet = {
        DataSet(mapToRDD(trainingDataPath), mapToRDD(predictionDataPath), outputPath)
      }

      def mapToRDD(path: String): RDD[Rating] = {
        sqlContext.read.parquet(path) map {
          case Row(unique_id: Long, game_id: String, saving: Int) => Rating(unique_id.toInt, game_id.toInt, saving.toDouble)
        } persist()
      }
    }

    lazy val dataSets: List[DataSet] = List(
      /*
        DataSet(
          "hdfs://pubgame/user/vincent/pg_user_game_90_training_v3.csv",
          "hdfs://pubgame/user/vincent/pg_user_game_90_other.csv",
          "hdfs://pubgame/user/vincent/spark-als"),
        DataSet(
          "hdfs://pubgame/user/vincent/pg_user_game_90_training_play.csv",
          "hdfs://pubgame/user/vincent/pg_user_game_90_other_play.csv",
          "hdfs://pubgame/user/vincent/spark-als-play"),
      DataSet(
        "hdfs://pubgame/user/terry/training90_ok_has_id.csv",
        "hdfs://pubgame/user/terry/testing90_ok_has_id.csv",
        "hdfs://pubgame/user/vincent/spark-als-90-all")
        */
      DataSet_(
        "s3n://data.emr/pg_user_game_90_training_v3.csv",
        "s3n://data.emr/pg_user_game_90_other.csv",
        "/home/hadoop/output/als-90")
    )

    lazy val dataFrames: List[DataSet] = List(
      DataFrame_("user_game_als_90", "user_game_als_not_90", "hdfs://pubgame/user/vincent/spark-als-all"))
    val fileSystem: FileSystem = FileSystem.get(new Configuration)
    //val delete_out_path: String = "hadoop fs -rm -f -r " + OUTPUT_PATH
    //delete_out_path.!

    val parametersSeq: IndexedSeq[(AlsParameters, DataSet)] = for {
      rank <- 2 until 50 by 2
      lambda <- 0.0001 until 15 by 0.1
      alpha <- 0.0001 until 50 by 0.1
      dataSet <- dataSets
    } yield (AlsParameters(rank = rank, lambda = lambda, alpha = alpha), dataSet)

    val futures: IndexedSeq[Future[Unit]] = Random.shuffle(parametersSeq).zipWithIndex.take(1) map {
      case ((parameters, dataSet), index) =>
        val trainingData: RDD[Rating] = dataSet.trainingData
        val predictionData: RDD[Rating] = dataSet.predictionData
        val outputPath: String = dataSet.outputPath
        case class Prediction(_1: RDD[Rating], _2: RDD[Rating], _3: RDD[Rating], _4: RDD[Rating])
        val split: Prediction = predictionData.randomSplit(Array.fill(4)(0.25), Platform.currentTime) match {
          case Array(split_1, split_2, split_3, split_4) => Prediction(split_1, split_2, split_3, split_4)
        }

        val evaluateModel_1: Future[Evaluation] = evaluateModel(trainingData union split._2 union split._3 union split._4, split._1, parameters)
        val evaluateModel_2: Future[Evaluation] = evaluateModel(trainingData union split._1 union split._3 union split._4, split._2, parameters)
        val evaluateModel_3: Future[Evaluation] = evaluateModel(trainingData union split._1 union split._2 union split._4, split._3, parameters)
        val evaluateModel_4: Future[Evaluation] = evaluateModel(trainingData union split._1 union split._2 union split._3, split._4, parameters)

        for {
          evaluation_1: Evaluation <- evaluateModel_1
          evaluation_2: Evaluation <- evaluateModel_2
          evaluation_3: Evaluation <- evaluateModel_3
          evaluation_4: Evaluation <- evaluateModel_4
        } yield {
          //ID,Average,Difference,Rank,Lambda,Alpha,Evaluation
          val recalls: List[Double] = List(evaluation_1.recall, evaluation_2.recall, evaluation_3.recall, evaluation_4.recall)
          val average: String = "%.4f".format(recalls.sum / recalls.length)
          val difference: String = "%.4f".format(recalls.max - recalls.min)
          val header: String = "%2d,%s,%s,%2d,%07.4f,%07.4f".format(index, average, difference, parameters.rank, parameters.lambda, parameters.alpha)
          val result: String = s"$header,$evaluation_1\r\n" +
            s"$header,$evaluation_2\r\n" +
            s"$header,$evaluation_3\r\n" +
            s"$header,$evaluation_4\r\n"
          Logger.log.warn("Sum:" + result)
          write(s"$outputPath/${System.currentTimeMillis}", result)
        }
    }
    Await.result(Future.sequence(futures), Duration.Inf)
  }

  def logSpace(min: Double, max: Double, count: Int): immutable.IndexedSeq[Double] = {
    (math.log(min) until math.log(max) by (math.log(max) - math.log(min)) / count).map(math.exp)
  }

  def evaluateModel(trainingData: RDD[Rating], testingData: RDD[Rating], parameters: AlsParameters): Future[Evaluation] = Future {
    Logger.log.warn("Evaluate")
    val predictResult: RDD[PredictResult] = ALS.trainImplicit(trainingData, parameters.rank, 40, parameters.lambda, parameters.alpha)
      .predict(testingData.map(dataSet => (dataSet.user, dataSet.product)))
      .map(predict => ((predict.user, predict.product), predict.rating))
      .join(testingData.map(dataSet => ((dataSet.user, dataSet.product), dataSet.rating))) map {
      case ((user, product), (predict, fact)) => PredictResult(user, product, predict, fact)
    }
    val evaluation: ConfusionMatrixResult = calConfusionMatrix(predictResult)
    val output: String = evaluation.toListString
    Logger.log.warn("Single:" + output)
    Evaluation(predictResult, output, evaluation.recall)
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
    val accuracy: Double = (result.tp + result.tn) / (p + n) //準確度
    val precision: Double = result.tp / (result.tp + result.fp) //猜顯性但真正是顯性的比率
    val recall: Double = result.tp / p //猜顯性正確率
    val fallout: Double = result.fp / n //猜顯性錯誤率
    val sensitivity: Double = result.tp / (result.tp + result.fn)
    val specificity: Double = result.tn / (result.fp + result.tn) //猜隱性正確的比率
    val f: Double = 2 * ((precision * recall) / (precision + recall))
    ConfusionMatrixResult(accuracy, precision, recall, fallout, sensitivity, specificity, f)
  }

  def write(path: String, content: String): Unit = {
    //val printWriter: PrintWriter = new PrintWriter(fileSystem.create(new Path(s"$outputPath/${System.nanoTime}")))
    val printWriter: PrintWriter = new PrintWriter(new FileOutputStream(path, true))
    try {
      printWriter.append(content)
      printWriter.println()
    } catch {
      case e: Exception => Logger.log.error(e.printStackTrace())
    } finally printWriter.close()
  }
}