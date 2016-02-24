import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD

import scala.collection.Map
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
  case class Evaluation(model: KMeansModel,
                        cluster: Int,
                        recall: Double,
                        targetResult: RDD[Int],
                        targetCountByValue: Map[Int, Long],
                        testResult: RDD[Int],
                        testCountByValue: Map[Int, Long])

  val counter: AtomicInteger = new AtomicInteger
  val bestEvaluation: Evaluation = (10 to 30).par.map {
    numClusters =>
      //Logger.log.warn(s"Now Iteration is [$numClusters], total is [${counter.incrementAndGet}]")
      val model: KMeansModel = KMeans.train(trainingData, numClusters, numIterations)
      val targetResult: RDD[Int] = targetUsers.mapValues(model.predict).values
      val targetCountByValue: Map[Int, Long] = targetResult.countByValue
      val targetMaxCluster: Int = targetCountByValue.maxBy(_._2)._1

      val testResult: RDD[Int] = testData.map(model.predict)
      val testCountByValue: Map[Int, Long] = testResult.countByValue
      val testMaxCluster: Int = testCountByValue.maxBy(_._2)._1

      val recall: Double = testCountByValue.maxBy(_._2)._2.toDouble / testResult.count.toDouble
      Logger.log.warn(s"When cluster = $numClusters, recall = $recall")
      Evaluation(model, numClusters, if (targetMaxCluster == testMaxCluster) recall else 0, targetResult, targetCountByValue, testResult, testCountByValue)
  } maxBy (_.recall)

  /*
  val bestModel: (KMeansModel, Double) = models.map(model => {
    val cost: Double = model.computeCost(trainingData)
    Logger.log.warn(s"When k = ${model.k}, cost = $cost")
    (model, cost)
  }).minBy(_._2)
  Logger.log.warn("Best Number of Cluster = " + bestModel._1.k) //112
  Logger.log.warn("Within Set Sum of Squared Errors = " + bestModel._2)
  */
  trainingData.unpersist()

  val path1: String = "hadoop fs -rm -f -r " + OutputModelPath
  path1.!
  val path2 = "hadoop fs -rm -f -r " + OutputPath
  path2.!
  val path3 = "hadoop fs -rm -f -r " + TargetPath
  path3.!
  val path4 = "hadoop fs -rm -f -r " + TestPath
  path4.!


  val allResult: RDD[(Long, Int)] = userVectors.mapValues(bestEvaluation.model.predict)

  //parsedData zip parsedDataExceptId map {
  //case (vectorWithId, vectorExceptId) => Vectors.dense(vectorWithId.toArray :+ bestModel._1.predict(vectorExceptId).toDouble)
  //}

  // Save and load model
  //clusters.save(sc, OUTPUT_PATH)
  //val sameModel = KMeansModel.load(sc, "myModelPath")

  Logger.log.warn("Cluster is：" + bestEvaluation.cluster)

  Logger.log.warn("All User Size：" + allResult.count)
  Logger.log.warn("All User Clustering Result：" + allResult.values.countByValue)

  Logger.log.warn("Target User Size：" + bestEvaluation.targetResult.count)
  Logger.log.warn("Target User Clustering Result：" + bestEvaluation.targetCountByValue) //testResult.map(_ -> 1).reduceByKey(_ + _)

  Logger.log.warn("Test User Size：" + bestEvaluation.testResult.count)
  Logger.log.warn("Test User Clustering Result：" + bestEvaluation.testCountByValue)
  //testResult.map(_ -> 1).reduceByKey(_ + _)

  //Logger.log.warn("In test user, is max value of cluster same as target user ?：" + (targetMaxCluster._1 == testMaxCluster._1))
  Logger.log.warn("Recall is：" + bestEvaluation.recall)

  bestEvaluation.model.save(sc, OutputModelPath)
  allResult.saveAsObjectFile(OutputPath)
  bestEvaluation.targetResult.saveAsObjectFile(TargetPath)
  bestEvaluation.testResult.saveAsObjectFile(TestPath)
}