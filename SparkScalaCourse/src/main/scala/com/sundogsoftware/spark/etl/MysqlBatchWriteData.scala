package com.sundogsoftware.spark.etl

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.sql.{Connection, PreparedStatement}
import java.util.Properties
import scala.collection.mutable.ListBuffer

object MysqlBatchWriteData {

  // Set the log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  case class Person(id: String, name: String, age: String, friends: String)

  def main(args: Array[String]): Unit = {

    //Creating a sparksession object
    val conf = new SparkConf()
      .setAppName("BatchInsertMySQL")
      .setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    import spark.implicits._
    //MySQL connection parameters
    val url = JDBCUtils.url
    val user = JDBCUtils.user
    val pwd = JDBCUtils.password

    //Create the properties object and set the user name and password to connect to MySQL
    val properties: Properties = new Properties()

    properties.setProperty("user", user) // user name
    properties.setProperty("password", pwd) // password
    properties.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    //      properties.setProperty("driver", "com.mysql.jdbc.Driver")
    properties.setProperty("numPartitions", "10")

    //Reading table data in MySQL
    val testDF: DataFrame = spark.read.jdbc(url, JDBCUtils.table, properties)
    println("number of partitions in testdf):" + testDF.rdd.partitions.size)
    testDF.createOrReplaceTempView("fakefriends")
    testDF.persist(StorageLevel.MEMORY_AND_DISK)
    testDF.printSchema()

    val result = s"select * from fakefriends"
    //        S' '-- SQL code
    //        """.stripMargin

    val resultBatch = spark.sql(result).as[Person]
    println(s"number of partitions in resultbatch: ${resultBatch.rdd.partitions.size}")

    //Batch write to MySQL
    //Here, it is best to re partition the processed results
    //Due to the large amount of data, there will be a lot of data in each partition
    resultBatch.rdd.repartition(500).foreachPartition(record => {

      val list = new ListBuffer[Person]

      //      for (person <- record.toList) {
      //        list.append(person)
      //      }
      record.foreach(person => {
        //        val name = person.name
        //        val age = person.age
        list.append(person)
      })
      upsertPerson(list) // batch insert data
    })

    //Method of batch inserting MySQL
    def upsertPerson(list: ListBuffer[Person]): Unit = {

      var connect: Connection = null
      var pstmt: PreparedStatement = null

      try {
        connect = JDBCUtils.getConnection()
        //Disable auto submit
        connect.setAutoCommit(false)

        val sql = "REPLACE INTO `fakefriends`(id, name, age, friends)" +
          " VALUES(?, ?, ?, ?)"

        pstmt = connect.prepareStatement(sql)

        var batchIndex = 0
        for (person <- list) {
          pstmt.setString(1, person.id+"china")
          pstmt.setString(2, person.name)
          pstmt.setString(3, person.age)
          pstmt.setString(4, person.friends.toString)
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

    spark.close()
  }
}
