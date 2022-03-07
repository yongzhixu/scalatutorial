package com.sundogsoftware.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.util.Properties

object _4_StreamingMysql {
  case class Person(name: String, age: Int)
  def main(args: Array[String]): Unit = {

//    val spark = SparkSession
//      .builder
//      .appName("_4_StreamingMysql")
//      .master("local[*]")
//      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
//      .config("spark.sql.streaming.checkpointLocation", "file:///C:/checkpoint")
//      .getOrCreate()

    //Creating a sparksession object
    val conf = new SparkConf()
      .setAppName("_4_StreamingMysql")
      .setMaster("local[*]")
    val spark: SparkSession =  SparkSession.builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._
    //MySQL connection parameters
    val url = JDBCUtils.url
    val user = JDBCUtils.user
    val pwd = JDBCUtils.password

    //Create the properties object and set the user name and password to connect to MySQL
    val properties: Properties = new Properties()

    properties.setProperty ("user", user) // user name
    properties.setProperty ("password", pwd) // password
    properties.setProperty("driver", "com.mysql.jdbc.Driver")
    properties.setProperty("numPartitions","10")

    //Reading table data in MySQL
    val testDF: DataFrame = spark.read.jdbc(url, "dfs_webhook_retry", properties)
    println("number of partitions in testdf):"+ testDF.rdd.partitions .size)
    testDF.createOrReplaceTempView("test")
    testDF.persist(StorageLevel.MEMORY_AND_DISK)
    testDF.printSchema()
    testDF.show()

  }
  def runJdbcDatasetExample(spark: SparkSession): Unit = {
    //    convertlab, localost(ConvertLab@Mysql)
    //Load data from JDBC source
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://127.0.0.1:3306/dfs")
      .option("dbtable", "customer_abc")
      .option("user", "convertlab")
      .option("password", "ConvertLab@Mysql")
      .load()

    jdbcDF.show()
//    jdbcDF.writeStream
//      .format("append")
//      .outputMode("console")

//    val connectionProperties = new Properties()
//    connectionProperties.put("user", "root")
//    connectionProperties.put("password", "root")
//    val jdbcDF2 = spark.read
//      .jdbc("jdbc:mysql://127.0.0.1:3306/test", "mytable", connectionProperties)
//    //Specifies the data type of the read schema
//    connectionProperties.put("customSchema", "id DECIMAL(38, 0), name STRING")
//    val jdbcDF3 = spark.read
//      .jdbc("jdbc:mysql://127.0.0.1:3306/test", "mytable", connectionProperties)

  }
}
