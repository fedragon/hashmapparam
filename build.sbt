name := "hashmapparam"

version := "1.0.0"

scalacOptions ++= Seq("-deprecation")

scalaVersion := "2.11.2"

libraryDependencies ++= Seq (
  "joda-time" % "joda-time" % "2.3",
  "org.joda" % "joda-convert" % "1.2",
  ("org.apache.spark" %% "spark-core" % "1.4.0" % "provided"),
  "junit" % "junit" % "4.11" % "test",
  "org.scalatest" %% "scalatest" % "2.2.0" % "test"
)
