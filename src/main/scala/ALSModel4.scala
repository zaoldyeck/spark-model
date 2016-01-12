import org.apache.spark.SparkContext

import scala.concurrent.duration.Duration
import scala.concurrent.{Future, Await}

/**
  * Created by zaoldyeck on 2016/1/12.
  */
class ALSModel4(implicit sc: SparkContext) extends ALSModel3 {
  override val dataSets: List[DataSet] = List(
    DataSet(
      "s3n://data.emr/train78ok.csv",
      "s3n://data.emr/test78ok.csv",
      "/home/hadoop/output/als-78")
  )

  override def run(): Unit = {
    val futures: List[Future[Unit]] = dataSets.map(dataSet => evaluateModel(dataSet.trainingData, dataSet.predictionData, AlsParameters) map {
      case Evaluation(output, recall) => Logger.log.warn(output)
    })
    Await.result(Future.sequence(futures), Duration.Inf)
  }
}