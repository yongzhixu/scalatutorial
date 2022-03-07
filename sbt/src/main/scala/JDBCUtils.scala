package com.sundogsoftware.spark.etl

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.mysql.cj.jdbc.Driver

object JDBCUtils {

  val database = "cd_vanguard_cdp"
  val user = "convertlab"
  val table = "fakefriends"
  val password = "ConvertLab@Mysql"
  val url: String = "jdbc:mysql://localhost:3306/" + database
//  Class.forName("com.mysql.jdbc.Driver")
  Class.forName("com.mysql.cj.jdbc.Driver")
  //Get a connection
  def getConnection() = {
    DriverManager.getConnection(url,user,password)
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
}
