package com.sundogsoftware.spark

import org.apache.spark.sql._
import org.apache.log4j._

object _5_SparkSQLDataset {

  case class Person(id: Int, name: String, age: Int, friends: Int)

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use SparkSession interface
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    // Load each line of the source data into an Dataset
    //    anytime you have spark implicitly infer a schema, you have to import spark.implicits._
    import spark.implicits._
    val schemaPeople = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[Person]

    schemaPeople.printSchema()

    //    create a database view
    schemaPeople.createOrReplaceTempView("people")

    schemaPeople.select(schemaPeople("name"), schemaPeople("age")+100).show()

    val teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

    val results = teenagers.collect()

    results.foreach(println)

    spark.stop()
  }
}