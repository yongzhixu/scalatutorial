package com.sundogsoftware.spark.exercise

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{round, max, avg}

object FriendsByAgeDatasetExercise {

  case class friend(id: Int, name: String, age: Int, friends: Int)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("FriendsByAgeDatasetExerciese")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val friendsDS = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("data/fakefriends.csv")
      .as[friend]

    <!-- use dataset api-->
    friendsDS
      .groupBy("age")
      .agg(
        round(avg("friends"), 2).alias("avg_friends")
        , max("id").alias("max_id")
        , max("name").alias("max_name")
      )
      .sort("avg_friends")
      .show()

    <!-- use createOrReplaceTempView-->
    friendsDS.createOrReplaceTempView("friendsView")

    spark.sql("select age, cast(avg(friends) as DECIMAL(8,2)) avg_friends from friendsView group by age order by avg_friends desc").show()

    spark.stop()
  }

}
