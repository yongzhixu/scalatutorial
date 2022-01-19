package com.sundogsoftware.spark.exercise

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{round, sum}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}


object CustomerTotalSpendDataSet {

  case class CustomerSpend(customer: String, item_id: Int, spend: Float)

  def parseLine(line: String): (String, Float) = {
    val fields = line.split(',')
    val customer = fields(0)
    val spend = fields(2).toFloat
    (customer, spend)
  }

  def main(args: Array[String]): Unit = {
    // set logger level
    Logger.getLogger("org").setLevel(Level.ERROR)

    //     create spark session
    val spark = SparkSession.builder
      .appName("CustomerTotalSpendDataSet")
      .master("local[*]")
      .getOrCreate()

    //    create schema
    val CustomerSpendSchema = new StructType()
      .add("customer", StringType, nullable = true)
      .add("item_id", IntegerType, nullable = true)
      .add("spend", FloatType, nullable = true)

    //    read data from csv file
    import spark.implicits._
    val dataSet = spark.read
      .schema(CustomerSpendSchema)
      .csv("data/customer-orders.csv")
      .as[CustomerSpend]

    <!-- using agg-->
    val totalSpendByCustomer = dataSet
      .select("customer", "spend")
      .groupBy("customer")
      .agg(round(sum("spend"), 2)
        .alias("total_spend"))
      .sort("total_spend")

//    totalSpendByCustomer.collect().foreach(println)
    for (spend <- totalSpendByCustomer.collect()) {
      println(s"${spend}, Customer ${spend(0)}, spent ${spend(1)} in total")
    }

    <!-- using sum-->
    val totalSpendByCustomerSum = dataSet
      .select("customer", "spend")
      .groupBy("customer")
      .sum("spend")

    totalSpendByCustomerSum.sort("sum(spend)").show(200)
  }
}
