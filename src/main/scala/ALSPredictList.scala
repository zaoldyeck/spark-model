import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame}

import scala.sys.process._
import scala.util.{Failure, Success, Try}

/**
  * Created by zaoldyeck on 2016/1/12.
  */

class ALSPredictList(gameId: Int, modelPath: String) extends ALSFold {
  private val PredictDataPath: String = "s3n://data.emr/all_id_except_90"
  val InputSchema = "parquet.`/user/kigo/kigo_1456315788061/modeling_data`"

  override def run(implicit sc: SparkContext): Unit = {
    val sqlContext = org.apache.spark.sql.SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val allUsersData: RDD[Rating] = loadLoginDataFromHive(InputSchema).persist
    val targetGamePlayUsersId: Array[Int] = getTargetGamePlayUsersId(InputSchema, gameId)
    val predictUsersData: RDD[(Int, Int)] = allUsersData.map(_.user).distinct.filter(!targetGamePlayUsersId.contains(_)).map(userId => (userId, gameId))
    val model: MatrixFactorizationModel = ALS.trainImplicit(allUsersData, 56, 100, 0.1662, 32.6604)
    model.save(sc, s"als/model/game$gameId/login")
    model.predict(predictUsersData).map(rating => User(rating.user, rating.product, rating.rating)).filter(_.pred > 0).toDF.registerTempTable("User")

    /* Old
    val predictRDD: RDD[(Int, Int)] = mappingPredictData(sc.textFile(PredictDataPath)).persist
    Logger.log.warn("Predict Size:" + predictRDD.count)

    val dataFrame: DataFrame = MatrixFactorizationModel.load(sc, modelPath).predict(predictRDD).map(rating => User(rating.user, rating.product, rating.rating)).toDF
    dataFrame.registerTempTable("User")
    */

    val delete_out_path2: String = s"hadoop fs -rm -f -r als/potential-users/game$gameId/login"
    delete_out_path2.!

    Logger.log.warn("start SparkSQL exec")
    //Logger.log.warn(s"write User table: \n${dataFrame.schema.treeString}")
    sqlContext.sql("select * from User").write.save(s"als/potential-users/game$gameId/login")

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

  def loadSavingDataFromHive(schema: String)(implicit sc: SparkContext): RDD[Rating] = {
    val sqlContext = org.apache.spark.sql.SQLContext.getOrCreate(sc)
    sqlContext.sql(s"select unique_id,game_index,revenue from $schema where web_mobile=2").rdd map {
      case Row(unique_id: Long, game_index: Int, revenue) =>
        Rating(unique_id.toInt, game_index, if (Some(revenue).getOrElse(0).asInstanceOf[Long] > 0) 1 else 0)
    }
  }

  def loadLoginDataFromHive(schema: String)(implicit sc: SparkContext): RDD[Rating] = {
    val sqlContext = org.apache.spark.sql.SQLContext.getOrCreate(sc)
    sqlContext.sql(s"select unique_id,game_index,unique_login_days from $schema where web_mobile=2").rdd map {
      case Row(unique_id: Long, game_index: Int, unique_login_days) =>
        Rating(unique_id.toInt, game_index, if (Some(unique_login_days).getOrElse(0).asInstanceOf[Long] >= 7) 1 else 0)
    }
  }

  def getTargetGamePlayUsersId(schema: String, targetGameId: Int)(implicit sc: SparkContext): Array[Int] = {
    val sqlContext = org.apache.spark.sql.SQLContext.getOrCreate(sc)
    //val allData: RDD[(Int, Int, Int)] = sqlContext.sql(s"select unique_id from parquet.`/user/kigo/kigo_1456315788061/modeling_data` where game_index=192").rdd map {
    sqlContext.sql(s"select unique_id from $schema where game_index=$targetGameId").rdd map {
      //val allData: RDD[(Int, Int, Int)] = sqlContext.sql(s"select unique_id,game_index,revenue from $schema where web_mobile=2").rdd map {
      case Row(unique_id: Long) => unique_id.toInt
    } collect
  }
}

case class User(player: Int, game: Int, pred: Double)