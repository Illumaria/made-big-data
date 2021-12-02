name := "05-linear-regression-spark-ml"

version := "0.1"

scalaVersion := "2.12.13"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-mllib" % "3.0.1" withSources(),
  "org.apache.spark" %% "spark-sql" % "3.0.1" withSources(),
  "org.scalatest" %% "scalatest" % "3.2.9" % "test" withSources()
)