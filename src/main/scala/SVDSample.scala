import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.log4j.{Level, LogManager}
import org.apache.spark._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import org.apache.commons.math3.linear._


import scala.reflect.ClassTag


class SVDSample {
  val log = LoggerFactory.getLogger(getClass)
  //private val InPath  = "./tl_training.txt"
  private val InPath = "./cray_als_web_not_only_tl_game_revised_filtered.txt"


  //private val InPathTest = "./pg_user_game_90_test_01.txt"

  private val OutRightSingularVectors = "./right_singular_vectors.txt"
  private val OutLeftSingularVectors = "./left_singular_vectors.txt"
  private val OutSingularVectors = "./singular_vectors.txt"

  def setSparkEnv(master: String): SparkContext = {
    LogManager.getRootLogger.setLevel(Level.WARN)
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName("SparkSVD")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("checkpoint/")
    sc.setLogLevel("WARN")
    sc
  }


  def dropHeader[T: ClassTag](data: RDD[T]): RDD[T] = {
    data.mapPartitionsWithIndex {
      case (0, lines) if lines.hasNext =>
        val header = lines.next()
        log.warn("header: {}", header)
        lines
      case (_, lines) =>
        lines
    }
  }


  def isNumeric(input: String): Boolean = input.forall(_.isDigit)


  def parseDouble(s: String) = {
    try {
      Some(s.toDouble)
    } catch {
      case _: Throwable => None
    }
  }


  case class Row(pubId: Int, gameId: Int, saving: Double)

  def load(data: RDD[String], dataName: String): RDD[Row] = {
    val dataRDD = data.flatMap(_.split(",") match {
      case Array(pubId, gameId, _, str_saving) => {

        val game_id_no_quotes = gameId.replaceAll("\"", "").toInt
        val option_saving = parseDouble(str_saving)

        for (s <- option_saving) yield
          Row(pubId.toInt, game_id_no_quotes, s)

      }
      case sucks => {
        log.warn("malformed data: {}", sucks)
        None
      }
    }).persist()
    dataRDD
  }




  def dataToMatrix(data:RDD[Row], games: RDD[Int], dataName:String)(implicit sc: SparkContext) ={

    //val transForm = sc.broadcast(games.zipWithIndex().collect().toMap.mapValues(s => if(s>0) 1 else 0)/* this fix java.io.NotSerializableException: scala.collection.immutable.MapLike$$anon$2*/.map(identity))
    val transForm = sc.broadcast(games.zipWithIndex().collect().toMap.mapValues(s => s.toInt)/* this fix java.io.NotSerializableException: scala.collection.immutable.MapLike$$anon$2*/.map(identity))
    val count = sc.broadcast(games.count().toInt)
    //val count = games.count().toInt

    // Construct rows of the RowMatrix
    val dataRows = data.groupBy(_.pubId).map {
      case (id, rows) =>
        val (indices, values) = rows.map {
          row =>
            transForm.value(row.gameId) -> row.saving
        }.unzip
        (id, (new SparseVector(count.value, indices.toArray, values.toArray)).asInstanceOf[Vector] ) // ( nCol, Array(col1, col2, ...), Array(value1, value2...) )
    }
    dataRows
  }




  def writeToFile(path: String, m:DenseMatrix): Unit = {

    val runingHadoopConfig = new Configuration()
    val runingHdfs = FileSystem.get(runingHadoopConfig)

    if (runingHdfs.exists(new Path(path))) {
      Logger.log.warn(s"file runing exists: ${path}")
    } else {
      val os = runingHdfs.create(new Path(path))
      val pw = new PrintWriter(os)
      try {

        val str_m = m.toArray.zipWithIndex.map {
          case (value, id) => {
            //Logger.log.warn(id+": "+value)
            if ( (id % m.numCols) == m.numCols-1)
              "%4.4f".format(value) + "\n"
            else
              "%4.4f".format(value) +  ", "
          }
        }
        pw.write(str_m.mkString(""))
      } finally {
        pw.close()
      }
    }
  }


  def writeToFile(path: String, m:RowMatrix): Unit = {

    val runingHadoopConfig = new Configuration()
    val runingHdfs = FileSystem.get(runingHadoopConfig)

    if (runingHdfs.exists(new Path(path))) {
      Logger.log.warn(s"file runing exists: ${path}")
    } else {
      val os = runingHdfs.create(new Path(path))
      val pw = new PrintWriter(os)
      try {


        val str_m = m.rows.flatMap {
          case row => row.toArray.zipWithIndex.map{
            case (value, id) => {
              //Logger.log.warn(id+": "+value)
              if ((id % m.numCols) == m.numCols - 1)
                "%4.4f".format(value) + "\n"
              else
                "%4.4f".format(value) + ", "
            }
          }
        }
        pw.write(str_m.collect().mkString(""))
      } finally {
        pw.close()
      }
    }
  }

  def matrixToRowMatrix(m: Matrix)(implicit sc:SparkContext): RowMatrix = {
    val columns = m.toArray.grouped(m.numRows)
    val rows = columns.toSeq.transpose // Skip this if you want a column-major RDD.
    val vectors = rows.map(row => Vectors.dense(row.toArray))
    val rowsRDD = sc.parallelize(vectors)
    val mat = new RowMatrix(rowsRDD)
    mat
  }



  def matrixToDenseMatrix(m: Matrix)(implicit sc:SparkContext): DenseMatrix = {
    val mat = new DenseMatrix(m.numCols, m.numRows, m.toArray)
    mat
  }



  def ExecSVD(implicit sc:SparkContext) = {
    log.info("Load into RDD...")
    val trainData = load(sc.textFile(InPath), "Training Data")
    //val testData = load(sc.textFile(InPathTest), "Testing Data")

    val games: RDD[Int] = trainData.map(_.gameId).distinct().persist()
    val dataRows = dataToMatrix(trainData, games, "Training Data")

    // Compute 20 largest singular values and corresponding singular vectors
    //val svd = new RowMatrix(dataRows.map(_._2.asInstanceOf[Vector]).persist()).computeSVD(20, computeU = true)
    val svd = new RowMatrix(dataRows.map(_._2).persist()).computeSVD(3, computeU = true)
    val rowMatrixV: Matrix = svd.V
    val transposeV = svd.V.transpose
    //val transposeS = DenseMatrix(svd.s).transpose
    val diagS =  DenseMatrix.diag(svd.s)
    val transV = svd.V.toArray.grouped(svd.V.numRows).toList.transpose
    Logger.log.warn(s"svd.U.numCols=${svd.U.numCols}, svd.U.numRows=${svd.U.numRows}")
    Logger.log.warn(s"svd.s.size = ${svd.s.size}")
    Logger.log.warn(s"svd.V.numCols=${svd.V.numCols}, svd.V.numRows=${svd.V.numRows}")
    Logger.log.warn(s"transposeV.numCols=${transposeV.numCols}, transposeV.numRows=${transposeV.numRows}")
    //Logger.log.warn(s"transposeS.numCols=${transposeS}, transposeV.numRows=${transposeV.numRows}")
    Logger.log.warn(s"S.numCols=${diagS.numCols}, S.numRows=${diagS.numRows}")
    Logger.log.warn(s"transV.size=${transV.size}")
    // Write results to hdfs

    /*
        svd.U.rows.map(row => row.toArray).zip(dataRows.map(_._1))
          .map(line => line._2 + "\t" + line._1.mkString("\t")) // make tsv line starting with row index
          .saveAsTextFile(OutLeftSingularVectors)

        sc.makeRDD(svd.s.toArray, 1)
          .saveAsTextFile(OutSingularVectors)

        sc.makeRDD(transV, 1).zip(games.repartition(1))
          .map(line => line._2 + "\t" + line._1.mkString("\t")) // make tsv line starting with column index
          .saveAsTextFile(OutRightSingularVectors)
        */


    //val newMatrix = svd.U.multiply(S).multiply(transposeV)
    val UserClusterMatrix: RowMatrix = svd.U.multiply(diagS)
    val ItemClusterMatrix  = diagS.multiply(matrixToDenseMatrix(svd.V))
    val AllMatrix = svd.U.multiply(diagS).multiply(svd.V.transpose)

    Logger.log.warn(s"UserClusterMatrix.numCols=${UserClusterMatrix.numCols}, UserClusterMatrix.numRows=${UserClusterMatrix.numRows}")

    Logger.log.warn(s"ItemClusterMatrix.numCols=${ItemClusterMatrix.numCols}, ItemClusterMatrix.numRows=${ItemClusterMatrix.numRows}")


    writeToFile("./LeftMatrix.txt", svd.U)
    writeToFile("./SingularMatrix.txt", new DenseMatrix(diagS.numCols, diagS.numRows, diagS.toArray) )
    writeToFile("./RightMatrix.txt", new DenseMatrix(svd.V.numCols, svd.V.numRows, svd.V.toArray) )

    writeToFile("./UserClusterMatrix.txt", UserClusterMatrix)
    writeToFile("./ItemClusterMatrix.txt", ItemClusterMatrix)
    writeToFile("./AllMatrix.txt", AllMatrix)

  }



  object Logger extends Serializable {
    val logger = LogManager.getRootLogger

    lazy val log = this.logger
  }


  def main(args: Array[String]) {

    val local_processors = s"local[${sys.runtime.availableProcessors()}]"
    Logger.log.warn(s"availableProcessors = $local_processors")
    implicit val sc = setSparkEnv(args.lastOption.getOrElse(local_processors))

    try {
      ExecSVD
    } finally {
      sc.stop()
    }
  }
}