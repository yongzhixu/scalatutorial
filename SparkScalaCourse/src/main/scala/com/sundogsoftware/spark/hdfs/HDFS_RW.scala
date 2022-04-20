package com.sundogsoftware.spark.hdfs

import org.apache.log4j.LogManager
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object HDFS_RW {

  case class HelloWorld(message: String)

  def main(args: Array[String]): Unit = {

    val log = LogManager.getRootLogger
    // Creation of Spark Session
    val sparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._

//    hdfs://hdfshost:8020/
//    val hdfs_master = args(0)
    val hdfs_master = "hdfs://192.168.50.202:8020/"
    // ====== Creating a dataframe with 1 partition
    val df = Seq(HelloWorld("helloworld")).toDF().coalesce(1)

    // ======= Writing files
    // Writing file as parquet
//    df.write.mode(SaveMode.Overwrite).parquet(hdfs_master + "lab/data/testwiki")
    //  Writing file as csv
//    df.write.mode(SaveMode.Overwrite).csv(hdfs_master + "lab/data/testwiki4.csv")

    // ======= Reading files
    // Reading parquet files
//    val df_parquet = sparkSession.read.parquet(hdfs_master + "lab/data/testwiki")
//    log.info(df_parquet.show())
    //  Reading csv files
//    val df_csv = sparkSession.read.option("inferSchema", "true").csv(hdfs_master + "lab/data/testwiki.csv")
//    log.info(df_csv.show())

    val ds:Dataset[String] = sparkSession.read.textFile(hdfs_master + "lab/sample.txt")

    ds.show()
  }
}
