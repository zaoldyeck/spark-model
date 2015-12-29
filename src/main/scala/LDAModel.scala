import akka.event.slf4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by zaoldyeck on 2015/12/28.
  */
class LDAModel {
  val INPUT_PATH = "hdfs://pubgame/user/vincent/efunfun_android_prod_game_for_kmeans.csv"
  val OUTPUT_PATH = "hdfs://pubgame/user/vincent/lda"
  val akkaLogger = Logger("！！This Is Important Message！！")

  def run(sc: SparkContext): Unit = {
    // Load and parse the data
    val data = sc.textFile(INPUT_PATH)
    val parsedData = data.map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache
    // Index documents with unique IDs
    val corpus = parsedData.zipWithIndex.map(_.swap).cache()

    // Cluster the documents into three topics using LDA
    val ldaModel = new LDA().setK(3).run(corpus)

    // Output topics. Each is a distribution over words (matching word count vectors)
    println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")
    val topics = ldaModel.topicsMatrix
    for (topic <- Range(0, 3)) {
      print("Topic " + topic + ":")
      for (word <- Range(0, ldaModel.vocabSize)) {
        print(" " + topics(word, topic))
      }
      println()
    }

    ldaModel.

    // Save and load model.
    //ldaModel.save(sc, "myLDAModel")
    //val sameModel = DistributedLDAModel.load(sc, "myLDAModel")
  }
}
