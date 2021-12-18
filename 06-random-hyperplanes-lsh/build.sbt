name := "06-random-hyperplanes-lsh"

version := "0.1"

scalaVersion := "2.12.13"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-mllib" % "3.1.2" withSources(),
  "org.apache.spark" %% "spark-sql" % "3.1.2" withSources(),
  "org.scalatest" %% "scalatest" % "3.2.9" % "test" withSources()
)