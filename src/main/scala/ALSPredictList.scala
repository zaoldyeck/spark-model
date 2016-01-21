import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.sys.process._

/**
  * Created by zaoldyeck on 2016/1/12.
  */
case class PredResult(player: Int, game: Int, pred: Double)

class ALSPredictList extends ALSFold {

  private val TrainingDataPath: String = "s3n://data.emr/web_login_rate"
  private val PredictDataPath: String = "s3n://data.emr/no_king_web_hasid"
  private val OutputPath: String = "/home/hadoop/output/als-for-play.txt"

  override def run(implicit sc: SparkContext): Unit = {
    val trainingRDD: RDD[Rating] = mappingData(sc.textFile(TrainingDataPath)).persist
    val predictRDD: RDD[(Int, Int)] = mappingData2(sc.textFile(PredictDataPath)).persist
    Logger.log.warn("Training Size:" + trainingRDD.count)
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

    val delete_out_path: String = "hadoop fs -rm -f -r ./als-play"
    delete_out_path.!

    Logger.log.warn("start SparkSQL exec")
    val sqlContext = org.apache.spark.sql.SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val model: MatrixFactorizationModel = ALS.trainImplicit(trainingRDD, 4, 100, 0.1086, 3.2757)
    //model.save(sc, s"model_als_game78_login_rate/test")
    //val model: MatrixFactorizationModel = MatrixFactorizationModel.load(sc, "model_als_game78_login_rate/test")
    val formatedRatesAndPredsTable: DataFrame = model.predict(predictRDD).map(rating => PredResult(rating.user, rating.product, rating.rating)).toDF
    formatedRatesAndPredsTable.registerTempTable("pred_result")

    Logger.log.warn(s"write pred_result table: \n${formatedRatesAndPredsTable.schema.treeString}")

    val delete_out_path2: String = "hadoop fs -rm -f -r ./predict_list/game78willplay"
    delete_out_path2.!

    sqlContext.sql("select * from pred_result").write.save(s"predict_list/game78willplay")

    Logger.log.warn(s"SparkContext.getExecutorMemoryStatus: ${sc.getExecutorMemoryStatus}\n")
    Logger.log.warn(s"SparkContext.getExecutorStorageStatus: ${sc.getExecutorStorageStatus.mkString(",")}\n")

    //predictResult.saveAsTextFile("./als-play")
    //val evaluation: ConfusionMatrixResult = calConfusionMatrix(predictResult)
    //val header: String = "%6d,%2d,%07.4f,%07.4f".format(0, 10, 0.01, 0.01)
    //Logger.log.warn("Single:" + header + "," + result.filter(_.rating > 0).count)
    //write(OutputPath, header + "," + evaluation.toListString)
  }


  override def mappingData(data: RDD[String]): RDD[Rating] = {
    Logger.log.warn("Mapping...")

    data flatMap {
      _.split(",") match {
        case Array(unique_id, game_index, max_login_times, max_login_days, max_duration_sec, pay_times, saving, gender, theme, style, community, type1, type2, web_mobile, login_rate) =>
          val rating: Double = login_rate.toDouble
          Some(Rating(unique_id.toInt, game_index.toInt, if (max_login_days.toInt > 1 || rating > 1) 1 else 0))
        case some =>
          Logger.log.warn("data error:" + some.mkString(","))
          None
      }
    }
  }

  def mappingData2(data: RDD[String]): RDD[(Int, Int)] = {
    Logger.log.warn("Mapping...")

    data flatMap {
      _.split(",") match {
        case Array(unique_id, _*) =>
          Some((unique_id.toInt, 78))
        case some =>
          Logger.log.warn("data error:" + some.mkString(","))
          None
      }
    }
  }
}