package com.sundogsoftware.spark

import org.apache.spark.sql._
import org.apache.log4j._

object _5_SparkSQL {

  //  column name: datatype
  case class Person(ID: Int, name: String, age: Int, numFriends: Int)

  def mapper(line: String): Person = {
    val fields = line.split(',')

    val person: Person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
    person
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface in Spark 2.0
    //    similar to database session
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    val lines = spark.sparkContext.textFile("data/fakefriends.csv")
      //      drop the first line
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    val people = lines.map(mapper)

    // Infer the schema, and register the DataSet as a table.
    //    anytime you have spark implicitly infer a schema, you have to import spark.implicits._   .
    //        to see why, uncomment the following line
    import spark.implicits._
    //    convert RDD to spark dataset
    val schemaPeople = people.toDS

    schemaPeople.printSchema()

    schemaPeople.createOrReplaceTempView("people")

    // SQL can be run over DataFrames that have been registered as a table
    val teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

    val results = teenagers.collect()

    results.foreach(println)

    spark.stop()
  }
}