import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import scala.sys.process._

/**
  * Created by zaoldyeck on 2016/2/18.
  */
class SVD(implicit sc: SparkContext) extends Serializable {

  val userVectors: RDD[(Long, Vector)] = sc.objectFile[(Long, Vector)]("kmeans_user_games", 64)

  val targetUsers: RDD[(Long, Vector)] = userVectors.filter {
    case (id, vector) => vector(142) == 1
  }
  private val tatgetUsersId: Array[Long] = targetUsers.map(_._1).collect
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

  val s1: String = "hadoop fs -rm -f -r svd/v"
  s1.!
  val s2 = "hadoop fs -rm -f -r svd/u"
  s2.!
  val s3 = "hadoop fs -rm -f -r svd/test-u"
  s3.!
  val s4 = "hadoop fs -rm -f -r svd/s"
  s4.!

  val svd: SingularValueDecomposition[RowMatrix, Matrix] = new RowMatrix(userVectors.values).computeSVD(100, computeU = true)
  val V: List[List[Double]] = svd.V.toArray.grouped(svd.V.numRows).toList.transpose
  sc.makeRDD(V, 1).zipWithIndex()
    .map(line => line._2 + "," + line._1.mkString(",")) // make tsv line starting with column index
    .saveAsTextFile("svd/v")

  private val allUserMatrix: RDD[(Array[Double], Long)] = svd.U.rows.map(row => row.toArray).zip(userVectors.map(_._1))
  allUserMatrix
    .map(line => line._2 + "," + line._1.mkString(",")) // make tsv line starting with row index
    .saveAsTextFile("svd/u")

  allUserMatrix
    .filter(row => tatgetUsersId.contains(row._2))
    .map(line => line._2 + "," + line._1.mkString(","))
    .saveAsTextFile("svd/test-u")

  sc.makeRDD(svd.s.toArray, 1)
    .repartition(1)
    .saveAsTextFile("svd/s")
}
