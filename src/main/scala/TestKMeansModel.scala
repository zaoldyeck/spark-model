import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeansModel

/**
  * Created by zaoldyeck on 2016/2/17.
  */
class TestKMeansModel(implicit sc: SparkContext) {
  KMeansModel.load(sc, "kmeans_model1455636031304")
  //res0
}
