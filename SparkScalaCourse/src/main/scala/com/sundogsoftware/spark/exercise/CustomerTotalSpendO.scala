package com.sundogsoftware.spark.exercise

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object CustomerTotalSpendO {

  def parseLine(line: String): (String, Float) = {
    val fields = line.split(',')
    val customer = fields(0)
    val spend = fields(2).toFloat
    (customer, spend)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    //    read file data

    //    create a sparkContext
    val sc = new SparkContext("local[*]", "CustomerTotalSpend")

    //    read each from csv file
    val lines = sc.textFile("data/customer-orders.csv")

    //    parse lines
    val parsedLines = lines.map(parseLine)

    val totalSpentByCustomerSorted = parsedLines.reduceByKey((x, y) => x + y).collect()

    for (spend <- totalSpentByCustomerSorted.sortBy(x => x._2)) {
      println(s"${spend}, Customer ${spend._1}, spent ${spend._2} in total")
    }
  }
}
