import akka.event.slf4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import sys.process._

/**
  * Created by zaoldyeck on 2015/12/27.
  */
class KMeansModel {
  val INPUT_PATH = "hdfs://pubgame/user/vincent/efunfun_android_prod_game_for_kmeans.csv"
  val OUTPUT_PATH = "hdfs://pubgame/user/vincent/kmeans"
  val akkaLogger = Logger("！！This Is Important Message！！")

  def run(sc: SparkContext): Unit = {
    val data: RDD[String] = sc.textFile(INPUT_PATH)

    val parsedData = data.map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache()
    parsedData.foreach(vector => akkaLogger.warn(vector.toString))

    // Cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    akkaLogger.warn("Within Set Sum of Squared Errors = " + WSSSE)

    clusters.clusterCenters.foreach(vector => akkaLogger.warn(vector.toString))

    val s = "hadoop fs -rm -f -r " + OUTPUT_PATH
    s.!

    // Save and load model
    clusters.save(sc, OUTPUT_PATH)
    //val sameModel = KMeansModel.load(sc, "myModelPath")
  }
}

//Try LDA