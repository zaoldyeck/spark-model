import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{KMeansModel, KMeans}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.rdd.RDD
import scala.collection.immutable.Seq
import scala.compat.Platform
import sys.process._

/**
  * Created by zaoldyeck on 2015/12/27.
  */
class KMeansSample {
  //val INPUT_PATH = "hdfs://pubgame/user/vincent/efunfun_android_prod_game_for_kmeans.csv"
  val OUTPUT_PATH = "hdfs://pubgame/user/vincent/kmeans_user_game_result"
  val OutputModelPath = "hdfs://pubgame/user/vincent/kmeans_model"

  def run(implicit sc: SparkContext): Unit = {
    val userVectors: RDD[(Long, Vector)] = sc.objectFile[(Long, Vector)]("hdfs://pubgame/user/vincent/kmeans_user_games", 64)

    //val data: RDD[String] = sc.textFile(INPUT_PATH)

    //val parsedData = data.map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache

    //val parsedDataExceptId = parsedData.map(vector => Vectors.dense(vector.toArray.drop(1)))

    // Cluster the data into two classes using KMeans
    val numIterations: Int = 50

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    //    val models = for (numClusters <- 2 to 200) yield {
    //      KMeans.train(data.values, numClusters, numIterations)
    //    }
    val values: RDD[Vector] = userVectors.values.persist()
    values.checkpoint()
    values.count()
    val counter: AtomicInteger = new AtomicInteger
    val models: Seq[KMeansModel] = (2 to 200).par.map {
      numClusters =>
        Logger.log.warn(s"Now Iteration is [$numClusters], total is [${counter.incrementAndGet}]")
        KMeans.train(values, numClusters, numIterations)
    }.seq

    val bestModel: (KMeansModel, Double) = models.map(model => {
      val cost: Double = model.computeCost(values)
      Logger.log.warn(s"When k = ${model.k}, cost = $cost")
      (model, cost)
    }).minBy(_._2)
    Logger.log.warn("Best Number of Cluster = " + bestModel._1.k) //112
    Logger.log.warn("Within Set Sum of Squared Errors = " + bestModel._2)
    values.unpersist()

    val s = "hadoop fs -rm -f -r " + OUTPUT_PATH
    s.!

    bestModel._1.save(sc, OutputModelPath + Platform.currentTime)

    Logger.log.warn("Output Data")
    val result: RDD[(Long, Int)] = userVectors.mapValues(bestModel._1.predict)

    //parsedData zip parsedDataExceptId map {
    //case (vectorWithId, vectorExceptId) => Vectors.dense(vectorWithId.toArray :+ bestModel._1.predict(vectorExceptId).toDouble)
    //}

    // Save and load model
    //clusters.save(sc, OUTPUT_PATH)
    //val sameModel = KMeansModel.load(sc, "myModelPath")

    result.saveAsTextFile(OUTPUT_PATH)
  }
}

//Try LDA