package com.sundogsoftware.sparkstreaming.NoSql

import java.util
import java.util.Properties

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
object ClickHouseLoad {
  val properties = new Properties()
  properties.put("driver", "ru.yandex.clickhouse.ClickHouseDriver")
  properties.put("user", "<yourUserName>")
  properties.put("password", "<yourPassword>")
  properties.put("batchsize","100000")
  properties.put("socket_timeout","300000")
  properties.put("numPartitions","8")
  properties.put("rewriteBatchedStatements","true")

  val url = "jdbc:clickhouse://<yourUrl>:8123/default"
  val table = "<yourTableName>"

  def main(args: Array[String]): Unit = {
    val sc = new SparkConf()
    sc.set("spark.driver.memory", "1G")
    sc.set("spark.driver.cores", "4")
    sc.set("spark.executor.memory", "1G")
    sc.set("spark.executor.cores", "2")

    val session = SparkSession.builder().master("local[*]").config(sc).appName("write-to-ck").getOrCreate()

    val df = session.read.format("csv")
      .option("header", "true")
      .option("sep", ",")
      .option("inferSchema", "true")
      .load("<yourFilePath>")
      .selectExpr(
        "colName1",
        "colName2",
        "colName3"
    )
    .persist(StorageLevel.MEMORY_ONLY_SER_2)
    println(s"read done")

    df.write.mode(SaveMode.Append).option(JDBCOptions.JDBC_BATCH_INSERT_SIZE, 100000).jdbc(url, table, properties)
    println(s"write done")

    df.unpersist(true)
  }
}
