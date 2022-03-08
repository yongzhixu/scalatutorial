package com.sundogsoftware.sparkstreaming.jdbc

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.util
import java.util.Properties
import scala.collection.mutable.ListBuffer

object _4_StreamingMysql {
  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {

    //Creating a sparksession object
    val conf = new SparkConf()
      .setAppName("_4_StreamingMysql")
      .setMaster("local[*]")
      .set("spark.sql.warehouse.dir", "file:///C:/temp")
      .set("spark.sql.streaming.checkpointLocation", "file:///C:/checkpoint")
    val spark: SparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    //Reading table data in MySQL
    val testDF = JDBCUtils.queryTable("person", spark)
    //    println("number of partitions in testdf):" + testDF.rdd.partitions.size)
    testDF.createOrReplaceTempView("test")
    testDF.persist(StorageLevel.MEMORY_AND_DISK)
    //    testDF.printSchema()
    //    testDF.show()

    var predicates = Array("name='john'","age<2")
    JDBCUtils.queryTable("person", predicates, spark).show()
    val result = s"select * from test;"
    //      """.stripMargin

    val resultBatch = spark.sql(result).as[Person]
    //    println("number of partitions in resultbatch:" + resultBatch.rdd.partitions.size)

    //Batch write to MySQL
    //Here, it is best to re partition the processed results
    //Due to the large amount of data, there will be a lot of data in each partition
    val times: Long = System.nanoTime()
    resultBatch.repartition(500).rdd.foreachPartition(record => {

      val list = new ListBuffer[Person]
      record.foreach(person => {
        val name = person.name
        val age = person.age
        list.append(Person(name, age))
      })
      val personService: PersonService = new PersonService()
      personService.upsertPerson(list) // batch insert data
    })


    println(s"it takes ${System.nanoTime() - times} nano seconds")
    spark.close()

  }

}
