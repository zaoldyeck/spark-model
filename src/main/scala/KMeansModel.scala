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

  def run(sc: SparkContext): Unit = {
    val data: RDD[String] = sc.textFile(INPUT_PATH)

    val parsedData = data.map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache

    val parsedDataExceptId = parsedData.map(vector => Vectors.dense(vector.toArray.drop(1)))

    // Cluster the data into two classes using KMeans
    val numIterations = 20

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val models = for (numClusters <- 2 to 200) yield {
      KMeans.train(parsedDataExceptId, numClusters, numIterations)
    }

    val bestModel = models.map(model => (model, model.computeCost(parsedDataExceptId))).minBy(_._2)
    Logger.log.warn("Best Number of Cluster is = " + bestModel._2)
    Logger.log.warn("Within Set Sum of Squared Errors = " + bestModel._2)

    val s = "hadoop fs -rm -f -r " + OUTPUT_PATH
    s.!

    Logger.log.warn("Output Data")
    val result = parsedData zip parsedDataExceptId map {
      case (vectorWithId, vectorExceptId) => Vectors.dense(vectorWithId.toArray :+ bestModel._1.predict(vectorExceptId).toDouble)
    }

    // Save and load model
    //clusters.save(sc, OUTPUT_PATH)
    //val sameModel = KMeansModel.load(sc, "myModelPath")

    result.saveAsTextFile(OUTPUT_PATH)
  }
}

//Try LDA