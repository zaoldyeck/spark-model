import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD

import scala.util.Random

/**
  * Created by zaoldyeck on 2016/1/6.
  */
class ALSModel3 extends ALSModel {
  private val TRAINING_DATA_PATH = "hdfs://pubgame/user/vincent/pg_user_game_78_training_web.csv"
  private val PREDICTION_DATA_PATH = "hdfs://pubgame/user/vincent/pg_user_game_78_training_web.csv"
  private val OUTPUT_PATH = "hdfs://pubgame/user/vincent/spark-als"

  override def run(implicit sc: SparkContext): Unit = {
    val trainingData: RDD[Rating] = mappingData(sc.textFile(TRAINING_DATA_PATH))
    val predictionData: RDD[Rating] = mappingData(sc.textFile(PREDICTION_DATA_PATH))

    Random.shuffle(predictionData)
  }
}