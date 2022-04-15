ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"
val SparkVersion = "3.1.2"

lazy val root = (project in file("."))
  .settings(
    name := "SparkStreaming"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % SparkVersion,
  "org.apache.spark" %% "spark-sql" % SparkVersion,
  "org.apache.spark" %% "spark-mllib" % SparkVersion,
  "org.apache.spark" %% "spark-streaming" % SparkVersion,
  "org.twitter4j" % "twitter4j-core" % "4.0.4",
  "org.twitter4j" % "twitter4j-stream" % "4.0.4",
  "com.twitter" % "jsr166e" % "1.1.0",
  "com.datastax.spark" % "spark-cassandra-connector_2.12" % "3.1.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % SparkVersion,
  "org.apache.spark" %% "spark-streaming-flume" % "2.4.8",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % SparkVersion,
  "org.apache.spark" %% "spark-streaming-kinesis-asl" % SparkVersion,
  "org.apache.spark" %% "spark-sql" % SparkVersion,
  "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.1",
  "mysql" % "mysql-connector-java" % "8.0.28",
  "org.mongodb" % "bson" % "4.5.0",
  "com.clickhouse" % "clickhouse-jdbc" % "0.3.2-patch7",
  "org.apache.hbase" % "hbase-client" % "2.4.9"
)
