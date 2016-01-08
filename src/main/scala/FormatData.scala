import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by zaoldyeck on 2016/1/8.
  */
class FormatData(implicit sc: SparkContext) {

  def run(): Unit = {
    val sql = new SQLContext(sc)
    import sql.implicits._
    new HiveContext(sc).table("pg_with_gd_target_data").select(
      $"unique_id".cast("Int"),
      $"game_id".cast("Int"))
      //$"saving".cast("Double").when($"saving" > 0, 1).otherwise(2).as("saving"))
      .filter($"max_login_days".cast("Int") >= 1 and $"max_login_times".cast("Int") >= 2)
      .write.parquet("user_game")
  }
}