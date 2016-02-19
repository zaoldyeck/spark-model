import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD

import scala.collection.Map
import scala.collection.immutable.Seq
import scala.compat.Platform
import scala.sys.process._

/**
  * Created by zaoldyeck on 2015/12/27.
  */
class KMeansSample(implicit sc: SparkContext) extends Serializable {
  val InputPath = "kmeans_user_games"
  val OutputModelPath = "kmeans/model"
  val OutputPath = "kmeans/all_user_result"
  val TargetPath = "kmeans/game78_user"
  val TestPath = "kmeans/test_game78_user"

  val userVectors: RDD[(Long, Vector)] = sc.objectFile[(Long, Vector)](InputPath, 64)

  val targetUsers: RDD[(Long, Vector)] = userVectors.filter {
    case (id, vector) => vector(142) == 1
  }

  val nonTargetUsers: RDD[(Long, Vector)] = userVectors.filter {
    case (id, vector) => vector(142) != 1
  }

  val targetUsersSplit: Array[RDD[(Long, Vector)]] = targetUsers.randomSplit(Array(0.9, 0.1), Platform.currentTime)
  val trainingData: RDD[Vector] = (nonTargetUsers.values union targetUsersSplit(0).values).persist
  trainingData.checkpoint
  trainingData.count
  val testData: RDD[Vector] = targetUsersSplit(1).values.persist
  testData.checkpoint
  testData.count

  //val data: RDD[String] = sc.textFile(INPUT_PATH)

  //val parsedData = data.map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache

  //val parsedDataExceptId = parsedData.map(vector => Vectors.dense(vector.toArray.drop(1)))

  // Cluster the data into two classes using KMeans
  val numIterations: Int = 1000

  // Evaluate clustering by computing Within Set Sum of Squared Errors
  //    val models = for (numClusters <- 2 to 200) yield {
  //      KMeans.train(data.values, numClusters, numIterations)
  //    }
  val counter: AtomicInteger = new AtomicInteger
  val models: Seq[KMeansModel] = (20 to 20).par.map {
    numClusters =>
      Logger.log.warn(s"Now Iteration is [$numClusters], total is [${counter.incrementAndGet}]")
      KMeans.train(trainingData, numClusters, numIterations)
  }.seq

  val bestModel: (KMeansModel, Double) = models.map(model => {
    val cost: Double = model.computeCost(trainingData)
    Logger.log.warn(s"When k = ${model.k}, cost = $cost")
    (model, cost)
  }).minBy(_._2)
  Logger.log.warn("Best Number of Cluster = " + bestModel._1.k) //112
  Logger.log.warn("Within Set Sum of Squared Errors = " + bestModel._2)
  trainingData.unpersist()

  val s1: String = "hadoop fs -rm -f -r " + OutputModelPath
  s1.!
  val s2 = "hadoop fs -rm -f -r " + OutputPath
  s2.!
  val s3 = "hadoop fs -rm -f -r " + TargetPath
  s3.!
  val s4 = "hadoop fs -rm -f -r " + TestPath
  s4.!

  bestModel._1.save(sc, OutputModelPath)
  val allResult: RDD[(Long, Int)] = userVectors.mapValues(bestModel._1.predict)
  val targetResult: RDD[Int] = targetUsers.mapValues(bestModel._1.predict).values
  val testResult: RDD[Int] = testData.map(bestModel._1.predict)

  //parsedData zip parsedDataExceptId map {
  //case (vectorWithId, vectorExceptId) => Vectors.dense(vectorWithId.toArray :+ bestModel._1.predict(vectorExceptId).toDouble)
  //}

  // Save and load model
  //clusters.save(sc, OUTPUT_PATH)
  //val sameModel = KMeansModel.load(sc, "myModelPath")

  allResult.saveAsObjectFile(OutputPath)
  Logger.log.warn("All User Size：" + allResult.count)
  Logger.log.warn("All User Clustering Result：" + allResult.countByValue)

  targetResult.saveAsObjectFile(TargetPath)
  Logger.log.warn("Target User Size：" + targetResult.count)
  private val targetCountByValue: Map[Int, Long] = targetResult.countByValue
  Logger.log.warn("Target User Clustering Result：" + targetCountByValue) //testResult.map(_ -> 1).reduceByKey(_ + _)

  testResult.saveAsObjectFile(TestPath)
  Logger.log.warn("Test User Size：" + testResult.count)
  private val testCountByValue: Map[Int, Long] = testResult.countByValue
  Logger.log.warn("Test User Clustering Result：" + testCountByValue)
  //testResult.map(_ -> 1).reduceByKey(_ + _)
  private val targetMaxCluster: (Int, Long) = targetCountByValue.maxBy(_._2)
  private val testMaxCluster: (Int, Long) = testCountByValue.maxBy(_._2)
  Logger.log.warn("In test user, is max value of cluster same as target user ?：" + (targetMaxCluster._1 == testMaxCluster._1))
  Logger.log.warn("Recall is：" + testMaxCluster._2.toDouble / targetMaxCluster._1.toDouble)
}