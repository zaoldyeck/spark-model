name := "spark-model"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "latest.integration",
  "org.apache.spark" % "spark-mllib_2.10" % "latest.integration",
  "org.apache.spark" % "spark-hive_2.10" % "latest.integration"
)