name := "SparkScalaCourse"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.2",
  "org.apache.spark" %% "spark-sql" % "3.1.2",
  "org.apache.spark" %% "spark-mllib" % "3.1.2",
  "org.apache.spark" %% "spark-streaming" % "3.1.2",
  "org.apache.kudu" % "kudu-client" % "1.15.0",
  "org.apache.kudu" % "kudu-spark3_2.12" % "1.15.0",
  "org.twitter4j" % "twitter4j-core" % "4.0.4",
  "org.twitter4j" % "twitter4j-stream" % "4.0.4",
  "mysql" % "mysql-connector-java" % "8.0.26",
  "org.apache.kafka" %% "kafka" % "3.0.0",
  "org.apache.spark" %% "spark-streaming-flume" % "2.4.8",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.1.2",
  "org.apache.spark" %% "spark-streaming-kinesis-asl" % "3.1.2",
  "org.apache.kafka" %% "kafka" % "3.0.0"
)
