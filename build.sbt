name := "spark-model"

version := "1.0"

//scalaVersion := "2.11.7"
scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.2",
  "org.apache.spark" %% "spark-mllib" % "1.5.2",
  "org.apache.spark" %% "spark-hive" % "1.5.2"
)