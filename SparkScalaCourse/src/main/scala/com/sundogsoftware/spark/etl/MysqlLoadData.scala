package com.sundogsoftware.spark.etl

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import java.util.Properties

object MysqlLoadData {


  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .appName("MysqlLoadData")
      .master("local[*]")
      .getOrCreate()

    //    Setup database connection variables
    val database = "cd_vanguard_cdp"
    val table = "fakefriends"
    val user = "convertlab"
    val password = "ConvertLab@Mysql"
    val connString = "jdbc:mysql://localhost:3306/" + database


    //    Load data via JDBC

    val jdbcDF = (spark.read.format("jdbc")
      .option("url", connString)
      .option("dbtable", table)
      .option("user", user)
      .option("password", password)
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .load())

    //    Show the DataFrame
    jdbcDF.show()



    val connectionProperties = new Properties()
    connectionProperties.put("user", user)
    connectionProperties.put("password", password)
    val jdbcDF2 = spark.read
      .jdbc(connString, table, connectionProperties)
    jdbcDF2.show()
    //Specifies the data type of the read schema
    connectionProperties.put("customSchema", "bu_id DECIMAL(38, 0), coupon_name STRING")
    val jdbcDF3 = spark.read
      .jdbc(connString, table,connectionProperties)

    jdbcDF3.show()
  }
}
