import org.slf4j.LoggerFactory
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
  val sparkLogger = LoggerFactory.getLogger(getClass)

  def run(sc: SparkContext): Unit = {
    val data: RDD[String] = sc.textFile(INPUT_PATH)

    val parsedData = data.map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache()

    sparkLogger.debug("Print Input Data")
    parsedData.foreach(println)

    val parsedDataExceptId = parsedData.map(vector => Vectors.dense(vector.toArray.drop(1)))

    // Cluster the data into two classes using KMeans
    val numClusters = 10
    val numIterations = 20
    val clusters = KMeans.train(parsedDataExceptId, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedDataExceptId)
    sparkLogger.warn("Within Set Sum of Squared Errors = " + WSSSE)

    val s = "hadoop fs -rm -f -r " + OUTPUT_PATH
    s.!

    sparkLogger.debug("Print Output Data")
    parsedData zip parsedDataExceptId map {
      case (vectorWithId, vectorExceptId) => Vectors.dense(vectorWithId.toArray :+ clusters.predict(vectorExceptId).toDouble)
    } foreach println

    // Save and load model
    clusters.save(sc, OUTPUT_PATH)
    //val sameModel = KMeansModel.load(sc, "myModelPath")
  }
}

//Try LDA