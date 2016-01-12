name := "spark-model"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "latest.integration",
  "org.apache.spark" %% "spark-mllib" % "latest.integration",
  "org.apache.spark" %% "spark-hive" % "latest.integration"
)