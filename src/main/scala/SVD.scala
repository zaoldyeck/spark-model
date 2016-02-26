import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD

import scala.sys.process._

/**
  * Created by zaoldyeck on 2016/2/18.
  */
class SVD(implicit sc: SparkContext) extends Serializable {

  //userId, gameId, rating
  /*
  val data: RDD[(Int, Int, Int)] = sc.parallelize(Array((1, 1, 1), (2, 1, 5), (3, 1, 5)), 64)
  private val collect: Array[(Int, Int, Int)] = data.collect()
  private val gameIdMapping: Map[Int, Int] = collect.map(_._2).distinct.sorted.zipWithIndex.toMap
  private val groupBy: Map[Int, Array[(Int, Int, Int)]] = collect.groupBy(_._1)

  private val tuples: Seq[(Int, Vector)] = groupBy.map {
    case (userId, array: Array[(Int, Int, Int)]) =>
      (userId, Vectors.sparse(gameIdMapping.size, array.map {
        case (user, gameId, rating) =>
          (gameIdMapping.get(gameId).get, rating.toDouble)
      }))
  } toSeq

  private val parallelize: RDD[(Int, Vector)] = sc.parallelize(tuples)

  val x = Array((10, 20.0), (24, 34.0))

  Vectors.sparse(20, x)
  */

  //sc.parallelize(Array((1, 0), (2, 3)))

  val InputPath = "kmeans_user_games"
  val UPath = "svd/u"
  val SPath = "svd/s"
  val VPath = "svd/v"
  val TargetUPath = "svd/target-u"
  val K = 30
  val TargetGameId = 101
  //game 78 -> 142 ; game 90 -> 84 ; game 192 -> 101
  val PredictUserPath = "svd/predict"

  val userVectors: RDD[(Long, Vector)] = sc.objectFile[(Long, Vector)](InputPath, 64).persist
  userVectors.checkpoint
  userVectors.count

  val targetUsersVector: RDD[(Long, Vector)] = userVectors.filter {
    case (id, vector) => vector(TargetGameId) == 1
  }
  private val targetUsersId: Array[Long] = targetUsersVector.map(_._1).collect
  /*
  userVectors.map {
    case (id, sparse: SparseVector) =>
      val index = 0 +: sparse.indices.map(_ + 1)
      val value = id.toDouble +: sparse.values
      Vectors.sparse(sparse.size + 1, index, value)
    case (id, dense: DenseVector) =>
      val value = id.toDouble +: dense.values
      Vectors.dense(value)
  }
  */

  val path1 = "hadoop fs -rm -f -r " + UPath
  path1.!
  val path2 = "hadoop fs -rm -f -r " + SPath
  path2.!
  val path3: String = "hadoop fs -rm -f -r " + VPath
  path3.!
  val path4 = "hadoop fs -rm -f -r " + TargetUPath
  path4.!
  val path5 = "hadoop fs -rm -f -r " + PredictUserPath
  path5.!

  val svd: SingularValueDecomposition[RowMatrix, Matrix] = new RowMatrix(userVectors.values).computeSVD(K, computeU = true)

  private val allUsers: RDD[(Array[Double], Long)] = svd.U.rows.map(row => row.toArray).zip(userVectors.map(_._1))
  val allUsersSize: Long = allUsers.count

  private val targetUsers: RDD[(Array[Double], Long)] = allUsers.filter(row => targetUsersId.contains(row._2))

  private val games: RDD[(List[Double], Long)] = sc.makeRDD(svd.V.toArray.grouped(svd.V.numRows).toList.transpose).zipWithIndex()
  private val targetGame: (List[Double], Long) = games.filter(_._2 == TargetGameId).first
  private val targetGameMostImportantIndex: Int = targetGame._1.indexOf(targetGame._1.max)

  import scala.reflect._

  /*
  allUsers.sortBy {
    case (array, id) => array(targetGameMostImportantIndex)
  }(Ordering[Double].reverse, classTag[Double])
  */
  findBestPercentage(0.2)

  def findBestPercentage(percentage: Double): Unit = {

    val topPercentageUsers: RDD[((Array[Double], Long), Long)] = allUsers.sortBy({
      case (array, id) => array(targetGameMostImportantIndex)
    }, false).zipWithIndex.filter {
      case ((array, id), index) =>
        index + 1 <= (allUsersSize * percentage).toInt
    }

    val targetPercentageUser: ((Array[Double], Long), Long) = topPercentageUsers.takeOrdered(1)(Ordering[Long].reverse.on(_._2))(0)

    val predictUsers: RDD[(Array[Double], Long)] = allUsers.filter {
      case (array, id) =>
        array(targetGameMostImportantIndex) >= targetPercentageUser._1._1(targetGameMostImportantIndex)
    }

    val recall: Double = predictUsers.map {
      case (array, id) => if (targetUsersId.contains(id)) 1 else 0
    }.sum / targetUsers.count.toDouble

    Logger.log.warn(s"When percentage is:$percentage: ,recall is:$recall, predict user size is:${topPercentageUsers.count}")
    if (percentage > 0.01) findBestPercentage(percentage - 0.005)
  }

  /*
    private val topPercentageUsers: RDD[((Array[Double], Long), Long)] = allUsers.sortBy({
      case (array, id) => array(targetGameMostImportantIndex)
    }, false).zipWithIndex.filter {
      case ((array, id), index) =>
        index + 1 <= (allUsersSize * 0.1).toInt
    }

    //val theTop20User: ((Array[Double], Long), Long) = topPercentageUsers.max()(Ordering[Long].reverse.on(_._2))
    val targetPercentageUser: ((Array[Double], Long), Long) = topPercentageUsers.takeOrdered(1)(Ordering[Long].reverse.on(_._2))(0)

    private val predictUsers: RDD[(Array[Double], Long)] = allUsers.filter {
      case (array, id) =>
        array(targetGameMostImportantIndex) >= targetPercentageUser._1._1(targetGameMostImportantIndex)
    }

    private val recall: Double = predictUsers.map {
      case (array, id) => if (targetUsersId.contains(id)) 1 else 0
    }.sum / targetUsers.count.toDouble
    */

  /*
  private val recall: Double = (targetUsers.filter {
    case (array, id) => array.indexOf(array.max) == targetGameMostImportantIndex
  } count).toDouble / targetUsers.count.toDouble
*/

  //  Logger.log.warn("Recall is:" + recall)
  //
  //  Logger.log.warn("Now begin to predict")
  //
  //  Logger.log.warn("Predict user size is:" + topPercentageUsers.count)

  /*
  private val predictUsersId: RDD[Long] = allUsers.filter {
    case (array, id) => array.indexOf(array.max) == targetGameMostImportantIndex
  } values

  Logger.log.warn("Predict user size is:" + predictUsersId.count)
  predictUsersId.saveAsObjectFile(PredictUserPath)

  allUsers.map(line => line._2 + "," + line._1.mkString(",")) // make tsv line starting with row index
    .saveAsTextFile(UPath)

  targetUsers.map(line => line._2 + "," + line._1.mkString(",")).saveAsTextFile(TargetUPath)

  sc.makeRDD(svd.s.toArray).saveAsTextFile(SPath)

  games.map(line => line._2 + "," + line._1.mkString(",")) // make tsv line starting with column index
    .saveAsTextFile(VPath)
    */
}
