name := "MysqlBatchWrite"

version := "0.1"

scalaVersion := "2.11.8"



libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.5" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.5" % "provided",
  "org.apache.spark" %% "spark-mllib" % "2.4.5" % "provided",
  "org.apache.kudu" % "kudu-client" % "1.15.0" % "provided",
  "org.apache.kudu" % "kudu-spark3_2.12" % "1.15.0" % "provided",
  "org.twitter4j" % "twitter4j-core" % "4.0.4" % "provided",
  "org.twitter4j" % "twitter4j-stream" % "4.0.4" % "provided",
  "mysql" % "mysql-connector-java" % "8.0.26" % "provided"
)