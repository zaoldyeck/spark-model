
git pull
sbt clean package
spark-submit --class Main target/scala-2.10/spark_2.10-1.0.jar
#spark-submit --class Main target/scala-2.11/spark_2.11-1.0.jar