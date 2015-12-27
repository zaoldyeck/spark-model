git pull
sbt clean
sbt package
spark-submit --class Main target/scala-2.10/spark_2.10-1.0.jar