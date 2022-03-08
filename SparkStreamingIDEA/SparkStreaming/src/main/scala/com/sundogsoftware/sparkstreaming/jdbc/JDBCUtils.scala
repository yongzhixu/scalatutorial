package com.sundogsoftware.sparkstreaming.jdbc

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

object JDBCUtils {

  val user = "convertlab"
  val password = "ConvertLab@Mysql"
  val url = "jdbc:mysql://localhost:3306/dfs"
  Class.forName("com.mysql.cj.jdbc.Driver")

  //  Class.forName("com.mysql.jdbc.Driver")
  //Get a connection
  def getConnection() = {
    DriverManager.getConnection(url, user, password)
  }

  //Release the connection
  def closeConnection(connection: Connection, pstmt: PreparedStatement): Unit = {
    try {
      if (pstmt != null) {
        pstmt.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }


  def queryTable(tableName:String,spark:SparkSession): DataFrame={

    //MySQL connection parameters
    val url = JDBCUtils.url
    val user = JDBCUtils.user
    val pwd = JDBCUtils.password

    //Create the properties object and set the user name and password to connect to MySQL
    val properties: Properties = new Properties()

    properties.setProperty("user", user) // user name
    properties.setProperty("password", pwd) // password
    properties.setProperty("driver", "com.mysql.jdbc.Driver")
    properties.setProperty("numPartitions", "10")

    //Reading table data in MySQL
    val resultDF: DataFrame = spark.read.jdbc(url, tableName, properties)
    resultDF
  }
}
