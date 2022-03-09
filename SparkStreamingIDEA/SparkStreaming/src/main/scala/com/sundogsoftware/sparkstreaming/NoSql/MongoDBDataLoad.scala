package com.sundogsoftware.sparkstreaming.NoSql

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import com.sundogsoftware.sparkstreaming.Utilities
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.bson.Document

import scala.util.parsing.json.JSONObject

object MongoDBDataLoad {

  def main(args: Array[String]): Unit = {
    println(s"Before read ${Utilities.getCurrentTimeStampMilli()}")

    //    Step 1 Create Spark Session
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)
      .set("spark.mongodb.input.uri", "mongodb://127.0.0.1/customer.inventory")
      .set("spark.mongodb.output.uri", "mongodb://127.0.0.1/customer.inventory")
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val sc = spark.sparkContext

    //    set Directory path for input data file and use Spark Dataframe reader to read the input data file as Spark DataFrame
    //    val path = "/Users/hadoop/path-to-csv/abc.csv"
    val path = "data/abc.csv"

    val dataDF = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("sep", ",")
      .option("inferSchema", "true")
      .load(path)

    //    Set Mongo Write config
    //    In this configuration we are using the Mongo DB collection “employee” and writeConcern.w "majority".
    //    “majority” means Write operation returns acknowledgement after propagating to M-number of data-bearing voting members (primary and secondaries)
    val writeConfig = WriteConfig(Map("collection" -> "inventory", "writeConcern.w" -> "majority")
      , Some(WriteConfig(sc)))

    //    Iterate thorough each Spark partition and parse JSON string to Mongo DB Document.
    val sparkDocuments = dataDF.rdd.mapPartitions(partition => {
      val docs = partition.map(row => {
        //        Convert row into a Map[String, String]
        var colMap: Map[String, String] = Map()
        row.schema.map(_.name).map(col => {
          val colName = col.trim

          val colIndex = row.fieldIndex(colName)
          val colAnyVal = row.getAs[AnyVal](colIndex)
          var colVal = if (colAnyVal == null) "NULL" else colAnyVal.toString()
          if (colVal == null) {
            val nullCol = "NULL"
            colVal = nullCol.asInstanceOf[String]
          }

          if (colVal.toString() == "null") {
            //            logic
          } else {
            colMap = colMap ++ Map(colName -> colVal)
          }
        })

//        convert map to json
//        https://index.scala-lang.org/spray/spray-json/spray-json/1.2.5?binaryVersion=_2.10
        val json = JSONObject(colMap).toString()
        val parseJSON = Document.parse(json)
        parseJSON
      }).toIterator
      docs
    }
    )

//     write data into mongoDB collections
    MongoSpark.save(sparkDocuments,writeConfig)
    println(s"After Write 2 ${Utilities.getCurrentTimeStampMilli()}")
  }

}
