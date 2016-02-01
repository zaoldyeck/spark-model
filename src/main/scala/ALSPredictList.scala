import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.sys.process._
import scala.util.{Failure, Success, Try}

/**
  * Created by zaoldyeck on 2016/1/12.
  */

class ALSPredictList(gameId: Int, modelPath: String) extends ALSFold {
  private val PredictDataPath: String = "s3n://data.emr/all_id_except_90"

  override def run(implicit sc: SparkContext): Unit = {
    val sqlContext = org.apache.spark.sql.SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val predictRDD: RDD[(Int, Int)] = mappingPredictData(sc.textFile(PredictDataPath)).repartition(50).persist
    Logger.log.warn("Predict Size:" + predictRDD.count)

    /*
    val result: RDD[Rating] = new ALS()
      .setImplicitPrefs(true)
      .setRank(10)
      .setIterations(20)
      .setLambda(0.01)
      .setAlpha(0.01)
      .setNonnegative(true)
      .run(trainingRDD).predict(predictRDD)
    */

    val dataFrame: DataFrame = MatrixFactorizationModel.load(sc, modelPath).predict(predictRDD).map(rating => User(rating.user, rating.product, rating.rating)).toDF
    dataFrame.registerTempTable("User")

    val delete_out_path2: String = s"hadoop fs -rm -f -r ./predict_list/game$gameId"
    delete_out_path2.!

    Logger.log.warn("start SparkSQL exec")
    Logger.log.warn(s"write User table: \n${dataFrame.schema.treeString}")
    sqlContext.sql("select * from User").write.save(s"predict_list/game$gameId")

    Logger.log.warn(s"SparkContext.getExecutorMemoryStatus: ${sc.getExecutorMemoryStatus}\n")
    Logger.log.warn(s"SparkContext.getExecutorStorageStatus: ${sc.getExecutorStorageStatus.mkString(",")}\n")
  }

  def mappingPredictData(data: RDD[String]): RDD[(Int, Int)] = {
    Logger.log.warn("Mapping...")

    data flatMap {
      _.split(",") match {
        case Array(unique_id, _*) => Try(Some((unique_id.toInt, gameId))) match {
          case Success(rate) => rate
          case Failure(e) => None
        }
        case some =>
          Logger.log.warn("data error:" + some.mkString(","))
          None
      }
    }
  }
}

case class User(player: Int, game: Int, pred: Double)