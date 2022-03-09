package com.sundogsoftware.sparkstreaming.NoSql

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.SparkSession

object MongoDBDataRead {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoDBExample")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/customer.inventory")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/customer.inventory")
      .getOrCreate()

    val sc = spark.sparkContext


    // Read the data from MongoDB to a DataFrame
    val readConfig = ReadConfig(Map("uri" -> "mongodb://127.0.0.1/", "database" -> "customer", "collection" -> "inventory")) // 1)
    val inventory = MongoSpark.load(sc, readConfig)

    import spark.implicits._
    inventory.toDF().printSchema()
    inventory.toDF().show()
  }
}
