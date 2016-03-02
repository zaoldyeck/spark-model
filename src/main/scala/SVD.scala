import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.sys.process._

/**
  * Created by zaoldyeck on 2016/2/18.
  */
class SVD extends Serializable {

  def run(implicit sc: SparkContext): Unit = {
    //userId, gameId, rating
    /*
    val data: RDD[(Int, Int, Int)] = sc.parallelize(Array((1, 1, 1), (2, 1, 5), (3, 1, 5)), 64)
     val collect: Array[(Int, Int, Int)] = data.collect()
     val gameIdMapping: Map[Int, Int] = collect.map(_._2).distinct.sorted.zipWithIndex.toMap
     val groupBy: Map[Int, Array[(Int, Int, Int)]] = collect.groupBy(_._1)
  
     val tuples: Seq[(Int, Vector)] = groupBy.map {
      case (userId, array: Array[(Int, Int, Int)]) =>
        (userId, Vectors.sparse(gameIdMapping.size, array.map {
          case (user, gameId, rating) =>
            (gameIdMapping.get(gameId).get, rating.toDouble)
        }))
    } toSeq
  
     val parallelize: RDD[(Int, Vector)] = sc.parallelize(tuples)
  
    val x = Array((10, 20.0), (24, 34.0))
  
    Vectors.sparse(20, x)
    */

    //sc.parallelize(Array((1, 0), (2, 3)))

    //val InputPath = "kmeans_user_games"
    val InputSchema = "parquet.`/user/kigo/dataset/modeling_data`"
    val UPath = "svd/u"
    val SPath = "svd/s"
    val VPath = "svd/v"
    val TargetUPath = "svd/target-u"
    val K = 30
    val TargetGameId = 192
    val TargetGameIndex = 69
    /*
    game 78 -> 142
    game 90 -> 84
    game kill 192 -> 69
    game baby 95 -> 39
    game 大司馬 91 -> 35
    game 虛空鬥仙 144 -> 52
    game 大帝國 223 ->
    */
    val PredictUserPath = "svd/predict"

    //val userVectors: RDD[(Long, Vector)] = sc.objectFile[(Long, Vector)](InputPath, 64).persist
    //val userVectors: RDD[(Int, Vector)] = loadSavingDataFromHive(InputSchema).persist
    val userVectors: RDD[(Int, Vector)] = loadLoginDataFromHive(InputSchema).persist
    userVectors.checkpoint

    val targetUsersVector: RDD[(Int, Vector)] = userVectors.filter {
      case (id, vector) => vector(TargetGameIndex) > 1
    }
    val targetUsersId: Array[Int] = targetUsersVector.map(_._1).collect
    val targetGamePlayUsersId: Array[Int] = getTargetGamePlayUsersId(InputSchema, TargetGameId)
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

    val path1: String = "hadoop fs -rm -f -r " + UPath
    path1.!
    val path2: String = "hadoop fs -rm -f -r " + SPath
    path2.!
    val path3: String = "hadoop fs -rm -f -r " + VPath
    path3.!
    val path4: String = "hadoop fs -rm -f -r " + TargetUPath
    path4.!
    val path5: String = "hadoop fs -rm -f -r " + PredictUserPath
    path5.!

    val svd: SingularValueDecomposition[RowMatrix, Matrix] = new RowMatrix(userVectors.values).computeSVD(K, computeU = true)

    val allUsers: RDD[(Array[Double], Int)] = svd.U.rows.map(row => row.toArray).zip(userVectors.map(_._1)).persist
    val allUsersSize: Long = allUsers.count

    val targetUsers: RDD[(Array[Double], Int)] = allUsers.filter(row => targetUsersId.contains(row._2)).persist

    val games: RDD[(List[Double], Long)] = sc.makeRDD(svd.V.toArray.grouped(svd.V.numRows).toList.transpose).zipWithIndex
    val targetGame: (List[Double], Long) = games.filter(_._2 == TargetGameIndex).first
    val targetGameMostImportantIndex: Int = targetGame._1.indexOf(targetGame._1.max)

    /*
    allUsers.sortBy {
      case (array, id) => array(targetGameMostImportantIndex)
    }(Ordering[Double].reverse, classTag[Double])
    */
    findBestPercentage(0.01)
    //findBestPercentage(0.1)
    //findBestPercentage(0.0174)
    //findBestPercentage(0.011) //for kill

    def findBestPercentage(percentage: Double): Unit = {
      val topPercentageUsers: RDD[((Array[Double], Int), Long)] = allUsers.sortBy({
        case (array, id) => array(targetGameMostImportantIndex)
      }, false).zipWithIndex.filter {
        case ((array, id), index) =>
          index + 1 <= (allUsersSize * percentage).toInt
      } persist

      val targetPercentageUser: ((Array[Double], Int), Long) = topPercentageUsers.takeOrdered(1)(Ordering[Long].reverse.on(_._2))(0)
      val targetPercentageUserMostImportantIndexValue: Double = targetPercentageUser._1._1(targetGameMostImportantIndex)
      Logger.log.warn(s"Target percentage user game most important index value is:$targetPercentageUserMostImportantIndexValue")

      val predictUsers: RDD[(Array[Double], Int)] = allUsers.filter {
        case (array, id) =>
          array(targetGameMostImportantIndex) >= targetPercentageUserMostImportantIndexValue
      } persist()
      Logger.log.warn(s"Predict users size:${predictUsers.count}")

      val recall: Double = predictUsers.map {
        case (array, id) => if (targetUsersId.contains(id)) 1 else 0
      }.sum / targetUsers.count.toDouble

      Logger.log.warn(s"When percentage is:$percentage, recall is:$recall, predict user size is:${topPercentageUsers.count}")

      ///////////////////////////////////////////////////////////////
      /*
      Logger.log.warn(s"Now saving potential users")
      val sqlContext = org.apache.spark.sql.SQLContext.getOrCreate(sc)
      import sqlContext.implicits._
      val potentialUsers: RDD[User] = predictUsers.filter {
        case (array, id) => !targetGamePlayUsersId.contains(id)
      } map {
        case (array, id) => User(id, 192, 1)
      } persist()
      Logger.log.warn(s"Potential users size:${potentialUsers.count}")
      potentialUsers toDF() registerTempTable "User"
      potentialUsers.unpersist()

      val path6: String = s"hadoop fs -rm -f -r svd/potential/game192-login"
      path6.!

      Logger.log.warn("start SparkSQL exec")
      sqlContext.sql("select * from User").write.save(s"svd/potential/game192-login")

      Logger.log.warn(s"SparkContext.getExecutorMemoryStatus: ${sc.getExecutorMemoryStatus}\n")
      Logger.log.warn(s"SparkContext.getExecutorStorageStatus: ${sc.getExecutorStorageStatus.mkString(",")}\n")
      */
      ///////////////////////////////////////////////////////////////


      topPercentageUsers.unpersist()
      predictUsers.unpersist()
      if (percentage < 0.2) {
        if (targetPercentageUserMostImportantIndexValue > 0) findBestPercentage(percentage + 0.01)
        else findBestPercentage(percentage - 0.001)
      }
    }

    /*
    allUsers.map(line => line._2 + "," + line._1.mkString(",")) // make tsv line starting with row index
      .saveAsTextFile(UPath)

    targetUsers.map(line => line._2 + "," + line._1.mkString(",")).saveAsTextFile(TargetUPath)

    sc.makeRDD(svd.s.toArray).saveAsTextFile(SPath)

    games.map(line => line._2 + "," + line._1.mkString(",")) // make tsv line starting with column index
      .saveAsTextFile(VPath)
      */
  }

  def loadSavingDataFromHive(schema: String)(implicit sc: SparkContext): RDD[(Int, Vector)] = {
    val sqlContext = org.apache.spark.sql.SQLContext.getOrCreate(sc)
    //val allData: RDD[(Int, Int, Int)] = sqlContext.sql(s"select unique_id,game_index,revenue from parquet.`/user/kigo/kigo_1456315788061/modeling_data` where web_mobile=2").rdd map {
    val allData: RDD[(Int, Int, Int)] = sqlContext.sql(s"select unique_id,game_index,revenue from $schema where web_mobile=2").rdd map {
      case Row(unique_id: Long, game_index: Int, revenue) =>
        (unique_id.toInt, game_index, Some(revenue).getOrElse(0).asInstanceOf[Long].toInt)
    }
    val gameIdMapping: Map[Int, Long] = allData.map(_._2).distinct.sortBy(gameId => gameId).zipWithIndex.collect.toMap
    val groupByUser: RDD[(Int, Iterable[(Int, Int, Int)])] = allData.groupBy(_._1)
    groupByUser.map {
      case (userId, iterable: Iterable[(Int, Int, Int)]) =>
        (userId, Vectors.sparse(gameIdMapping.size, iterable.map {
          case (user, gameId, revenue) => (gameIdMapping.get(gameId).get.toInt, revenue.toDouble)
        } toSeq))
    }
  }

  def loadLoginDataFromHive(schema: String)(implicit sc: SparkContext): RDD[(Int, Vector)] = {
    val sqlContext = org.apache.spark.sql.SQLContext.getOrCreate(sc)
    //val allData: RDD[(Int, Int, Int)] = sqlContext.sql(s"select unique_id,game_index,revenue from parquet.`/user/kigo/dataset/modeling_data` where web_mobile=2").rdd map {
    val allData: RDD[(Int, Int, Int)] = sqlContext.sql(s"select unique_id,game_index,unique_login_days from $schema where web_mobile=2").rdd map {
      case Row(unique_id: Long, game_index: Int, unique_login_days) =>
        (unique_id.toInt, game_index, Some(unique_login_days).getOrElse(0).asInstanceOf[Long].toInt)
    }
    val gameIdMapping: Map[Int, Long] = allData.map(_._2).distinct.sortBy(gameId => gameId).zipWithIndex.collect.toMap
    val groupByUser: RDD[(Int, Iterable[(Int, Int, Int)])] = allData.groupBy(_._1)
    groupByUser.map {
      case (userId, iterable: Iterable[(Int, Int, Int)]) =>
        (userId, Vectors.sparse(gameIdMapping.size, iterable.map {
          case (user, gameId, revenue) => (gameIdMapping.get(gameId).get.toInt, revenue.toDouble)
        } toSeq))
    }
  }

  def getTargetGamePlayUsersId(schema: String, targetGameId: Int)(implicit sc: SparkContext): Array[Int] = {
    val sqlContext = org.apache.spark.sql.SQLContext.getOrCreate(sc)
    //val allData: RDD[(Int, Int, Int)] = sqlContext.sql(s"select distinct unique_id from parquet.`parquet.`/user/kigo/dataset/modeling_data`` where game_index=192").rdd map {
    sqlContext.sql(s"select distinct unique_id from $schema where game_index=$targetGameId").rdd map {
      //val allData: RDD[(Int, Int, Int)] = sqlContext.sql(s"select unique_id,game_index,revenue from $schema where web_mobile=2").rdd map {
      case Row(unique_id: Long) => unique_id.toInt
    } collect
  }
}