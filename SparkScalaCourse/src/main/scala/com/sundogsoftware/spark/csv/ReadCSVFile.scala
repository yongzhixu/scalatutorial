package com.sundogsoftware.spark.csv
import com.sundogsoftware.spark._2_FriendsByAgeDataset.FakeFriends
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object ReadCSVFile {

  case class Employee(empno: String, ename: String, designation: String, manager: String, hire_date: String, sal: String, deptno: String)

  def main(args: Array[String]): Unit = {

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("FriendsByAge")
      .master("local[*]")
      .getOrCreate()

    // Load each line of the source data into an Dataset
    import spark.implicits._
    val ds = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[FakeFriends]

    // Select only age and numFriends columns
    val friendsByAge = ds.select("age", "friends")

  }
}
