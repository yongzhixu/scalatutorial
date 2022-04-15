package com.sundogsoftware.sparkstreaming.NoSql

import com.clickhouse.jdbc.ClickHouseDataSource
import java.util.Properties
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object ClickHouseQuery {

  val properties = new Properties()
  properties.put("driver", "com.clickhouse.jdbc.ClickHouseDriver")
  properties.put("user", "default")
  //  properties.put("password", "<yourPassword>")
  properties.put("batchsize", "100000")
  properties.put("socket_timeout", "300000")
  properties.put("numPartitions", "8")
  properties.put("rewriteBatchedStatements", "true")

  val url = "jdbc:clickhouse://192.168.50.203:8123/tutorial"
  val table = "visits_v1"

  def main(args: Array[String]): Unit = {

    val query = "SELECT URL, MobilePhone FROM hits_v1 LIMIT 10"
    /*================================Using ClickHouseDataSource===============*/
    val chDatasource = new ClickHouseDataSource(url, properties)
    val chConn = chDatasource.getConnection()
    val psmt = chConn.prepareStatement("SELECT * FROM tutorial.hits_v1 LIMIT 10")
    val result = psmt.executeQuery()
    while (result.next()) {
      //query schema/metadata/ColumnName
      println("result " + result.getMetaData.getColumnCount)
      println("result " + result.getMetaData.getColumnName(1))
      for (x <- 1 to result.getMetaData.getColumnCount) {
        print(s"[$x](${result.getMetaData.getColumnName(x)})")
      }

      //query column value
      val URL = result.getString("URL")
      val MobilePhone = result.getString("MobilePhone")
      println("URL  " + URL)
      println("MobilePhone " + MobilePhone)
      println("result " + result.getString(3))
      for (x <- 1 to result.getMetaData.getColumnCount) {
        print(s"{${result.getMetaData.getColumnName(x)} :${result.getString(x)} }, ")
      }
    }

    /*================================Using SparkSession===============*/
    val sparkConf: SparkConf =
      new SparkConf()
        .setMaster("local[*]")
        .setAppName(this.getClass.getName)

    val spark: SparkSession =
      SparkSession
        .builder()
        //        .enableHiveSupport()
        .config(sparkConf)
        .getOrCreate()

    //    spark.read
    //      .format("jdbc")
    //      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
    //      .option("url", url)
    //      .option("fetchsize", "100000")
    //      .option("batchsize", "100000")
    //      .option("user", "default")
    ////      .option("numPartitions", "2")
    //      .option("dbtable", table)
    //      .load()

    spark.read
      .jdbc(url, "(SELECT URL,MobilePhone FROM hits_v1 limit 10)", properties)
      //      .where("MobilePhone>0")
      .show()

    spark.read
      .format("jdbc")
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .option("url", url)
      .option("user", "default")
      .option("password", "")
      .option("dbtable", "(SELECT URL,MobilePhone FROM hits_v1 limit 10)")
      .load()
      .show()
  }
}
