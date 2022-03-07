package com.sundogsoftware.sparkstreaming

import java.sql.{Connection, DriverManager, PreparedStatement}

object JDBCUtils {

  val user = "convertlab"
  val password = "ConvertLab@Mysql"
  val url = "jdbc:mysql://localhost:3306/dfs"
  Class.forName("com.mysql.cj.jdbc.Driver")
//  Class.forName("com.mysql.jdbc.Driver")
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
