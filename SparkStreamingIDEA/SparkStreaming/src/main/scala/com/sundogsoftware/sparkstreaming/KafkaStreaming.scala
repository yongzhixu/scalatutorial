package com.sundogsoftware.sparkstreaming

import com.sundogsoftware.sparkstreaming.Utilities._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.text.SimpleDateFormat
import java.util.Locale
import java.util.regex.{Matcher, Pattern}

object KafkaStreaming {

  // Case class defining structured data for a line of Apache access log data
  case class LogEntry(ip: String, client: String, user: String, dateTime: String, request: String, status: String, bytes: String, referer: String, agent: String)

  val logPattern = apacheLogPattern()
  val datePattern = Pattern.compile("\\[(.*?) .+]")

  // Function to convert Apache log times to what Spark/SQL expects
  def parseDateField(field: String): Option[String] = {

    val dateMatcher = datePattern.matcher(field)
    if (dateMatcher.find) {
      val dateString = dateMatcher.group(1)
      val dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
      val date = (dateFormat.parse(dateString))
      val timestamp = new java.sql.Timestamp(date.getTime());
      return Option(timestamp.toString())
    } else {
      None
    }
  }

  // Convert a raw line of Apache access log data to a structured LogEntry object (or None if line is corrupt)
  def parseLog(x: Row): Option[LogEntry] = {

    val matcher: Matcher = logPattern.matcher(x.getString(0));
    if (matcher.matches()) {
      val timeString = matcher.group(4)
      return Some(LogEntry(
        matcher.group(1),
        matcher.group(2),
        matcher.group(3),
        parseDateField(matcher.group(4)).getOrElse(""),
        matcher.group(5),
        matcher.group(6),
        matcher.group(7),
        matcher.group(8),
        matcher.group(9)
      ))
    } else {
      return None
    }
  }

  def main(args: Array[String]) {
    // Use new SparkSession interface in Spark 2.0


    val spark = SparkSession
      .builder
      .appName("KafkaStreaming")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .config("spark.sql.streaming.checkpointLocation", "file:///C:/checkpoint")
      .getOrCreate()

    setupLogging()
    import spark.implicits._
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("subscribe", "topic-amos-test-topic")
      .option("startingOffsets", "earliest") // From starting
      .load()
    df.printSchema()

//Spark Streaming Write to Console
    val personStringDF = df.selectExpr("CAST(value AS STRING)")

    val schema = new StructType()
      .add("id",IntegerType)
      .add("username",StringType)
      .add("firstName",StringType)
      .add("lastname",StringType)
      .add("email",StringType)
      .add("password",StringType)
      .add("phone",StringType)
      .add("userStatus",IntegerType)

    val personDF = personStringDF.select(from_json(col("value"), schema).as("data"))
      .select("data.*")

    personDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

    spark.stop()
  }
}