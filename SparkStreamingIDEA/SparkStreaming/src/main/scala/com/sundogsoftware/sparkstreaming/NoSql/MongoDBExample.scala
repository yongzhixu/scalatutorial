package com.sundogsoftware.sparkstreaming.NoSql

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.SparkSession

object MongoDBExample {
  def main(args: Array[String]): Unit = {
    /* Create the SparkSession.
     * If config arguments are passed from the command line using --conf,
     * parse args for the values to set.
     */
//    import org.apache.spark.sql.SparkSession
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
    inventory.toDF().printSchema()
    inventory.toDF().show()
  }
}
