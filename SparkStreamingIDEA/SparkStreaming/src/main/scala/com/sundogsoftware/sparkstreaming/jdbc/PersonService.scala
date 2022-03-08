package com.sundogsoftware.sparkstreaming.jdbc

import com.sundogsoftware.sparkstreaming.jdbc._4_StreamingMysql.Person

import java.sql.{Connection, PreparedStatement}
import scala.collection.mutable.ListBuffer

class PersonService {


  //Method of batch inserting MySQL
  def upsertPerson(list: ListBuffer[Person]): Unit = {

    var connect: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connect = JDBCUtils.getConnection()
      //Disable auto submit
      connect.setAutoCommit(false)

      val sql = "REPLACE INTO `person`(name, age)" +
        " VALUES(?, ?)"

      pstmt = connect.prepareStatement(sql)

      var batchIndex = 0
      for (person <- list) {
        pstmt.setString(1, person.name)
        pstmt.setInt(2, person.age)
        //Add batch
        pstmt.addBatch()
        batchIndex += 1
        //Control the number of submissions,
        //MySQL batch write try to limit the amount of submitted batch data, otherwise MySQL will be written to hang up!!!
        if (batchIndex % 1000 == 0 && batchIndex != 0) {
          pstmt.executeBatch()
          pstmt.clearBatch()
        }

      }
      //Submit batch
      pstmt.executeBatch()
      connect.commit()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      JDBCUtils.closeConnection(connect, pstmt)
    }
  }


}
