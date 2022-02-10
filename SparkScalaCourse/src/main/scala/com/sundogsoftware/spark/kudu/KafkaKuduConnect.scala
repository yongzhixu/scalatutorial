//package com.sundogsoftware.spark.kudu
//
//import org.apache.spark.streaming.kafka010._
//
//
//import kafka.serializer.StringDecoder
//import org.apache.kudu.client.KuduClient
//import org.apache.kudu.client.KuduSession
//import org.apache.kudu.client.KuduTable
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.streaming.Milliseconds
//import org.apache.spark.streaming.StreamingContext
////  import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.kafka010._
//
//
//import scala.collection.immutable.List
//import scala.collection.mutable
//import scala.util.control.NonFatal
//import org.apache.spark.sql.{Row, SQLContext}
//import org.apache.spark.sql.types._
//import org.apache.kudu.Schema
//import org.apache.kudu.Type._
//import org.apache.kudu.spark.kudu.KuduContext
//
//import scala.collection.mutable.ArrayBuffer
//
//object KafkaKuduConnect extends Serializable {
//
//  def main(args: Array[String]): Unit = {
//    try {
//      val TopicName = "TestTopic"
//      val kafkaConsumerProps = Map[String, String]("bootstrap.servers" -> "localhost:9092")
//      val KuduMaster = ""
//      val KuduTable = ""
//      val sparkConf = new SparkConf().setAppName("KafkaKuduConnect")
//
//      val sc = new SparkContext(sparkConf)
//
//      val sqlContext = new SQLContext(sc)
//
//      import sqlContext.implicits._
//
//      val ssc = new StreamingContext(sc, Milliseconds(1000))
//
//      val kuduContext = new KuduContext(KuduMaster, sc)
//
//      val kuduclient: KuduClient = new KuduClient.KuduClientBuilder(KuduMaster).build()
//      //Opening table
//      val kudutable: KuduTable = kuduclient.openTable(KuduTable)
//      // getting table schema
//      val tableschema: Schema = kudutable.getSchema
//      // creating the schema for the data frame using the table schema
//      val FinalTableSchema = generateStructure(tableschema)
//
//
//      //To create the schema for creating the data frame from the rdd
//      def generateStructure(tableSchema: Schema): StructType = {
//        var structFieldList: List[StructField] = List()
//        for (index <- 0 until tableSchema.getColumnCount) {
//          val col = tableSchema.getColumnByIndex(index)
//          val coltype = col.getType.toString
//          println(coltype)
//          col.getType match {
//            case INT32 =>
//              structFieldList = structFieldList :+ StructField(col.getName, IntegerType)
//            case STRING =>
//              structFieldList = structFieldList :+ StructField(col.getName, StringType)
//            case _ =>
//              println("No Class Type Found")
//          }
//        }
//        return StructType(structFieldList)
//      }
//
//      // To create the Row object with values type casted according to the schema
//      def getRow(schema: StructType, Data: List[String]): Row = {
//        var RowData = ArrayBuffer[Any]()
//        schema.zipWithIndex.foreach(
//          each => {
//            var Index = each._2
//            each._1.dataType match {
//              case IntegerType =>
//                if (Data(Index) == "" | Data(Index) == null)
//                  RowData += Data(Index).toInt
//
//              case StringType =>
//                RowData += Data(Index)
//              case _ =>
//                RowData += Data(Index)
//            }
//          }
//        )
//        return Row.fromSeq(RowData.toList)
//      }
//
//
//      val messages = KafkaUtils.createDirectStream[String, String](ssc, kafkaConsumerProps, Set(TopicName))
//      messages.foreachRDD(
//        //we are looping through eachrdd
//        eachrdd => {
//          //we are creating the Rdd[Row] to create dataframe with our schema
//          val StructuredRdd = eachrdd.map(eachmessage => {
//            val record = eachmessage._2
//            getRow(FinalTableSchema, record.split(",").toList)
//          })
//          //DataFrame with required structure according to the table.
//          val DF = sqlContext.createDataFrame(StructuredRdd, FinalTableSchema)
//          kuduContext.upsertRows(DF, KuduTable)
//        }
//      )
//
//    }
//    catch {
//      case NonFatal(e) => {
//        print("Error in main : " + e)
//      }
//    }
//  }
//}
