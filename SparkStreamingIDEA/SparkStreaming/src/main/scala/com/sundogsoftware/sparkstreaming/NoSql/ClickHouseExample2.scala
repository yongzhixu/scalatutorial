//package com.sundogsoftware.sparkstreaming.NoSql
//
//import java.util.Random
//
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.types.{DoubleType, LongType, StringType}
//import org.apache.spark.sql.{SaveMode, SparkSession}
//import ru.yandex.clickhouse.ClickHouseDataSource
//
//object ClickHouseExample2 {
//  val chDriver = "com.clickhouse.jdbc.ClickHouseDriver"
//  val chUrls = Array(
//    "jdbc:clickhouse://192.168.50.203:8123/default",
//    "jdbc:clickhouse://192.168.50.203:8123/default")
//
//  def main(args: Array[String]): Unit = {
//    if (args.length < 3) {
//      System.err.println("Usage: OfficialJDBCDriver <tableName> <partitions> <batchSize>\n" +
//        "  <tableName> is the hive table name \n" +
//        "  <partitions> is the partitions which want insert into clickhouse, like 20200516,20200517\n" +
//        "  <batchSize> is JDBC batch size, may be 1000\n\n")
//      System.exit(1)
//    }
//
//    val (tableName, partitions, batchSize) = (args(0), args(1).split(","),  args(2).toInt)
//    val sparkConf: SparkConf =
//      new SparkConf()
//        .setAppName("OfficialJDBCDriver ")
//
//    val spark: SparkSession =
//      SparkSession
//        .builder()
//        .enableHiveSupport()
//        .config(sparkConf)
//        .getOrCreate()
//
//    val pro = new java.util.Properties
//    pro.put("driver", chDriver)
//    pro.setProperty("user", "default")
//    pro.setProperty("password", "123456")
//    var chShardIndex = 1
//    for (partition <- partitions) {
//      val chUrl = chUrls((chShardIndex - 1) % chUrls.length)
//      val sql = s"select * from tmp.$tableName where dt = $partition"
//      val df = spark.sql(sql)
//      val (fieldNames, placeholders) = df.schema.fieldNames.foldLeft("", "")(
//        (str, name) =>
//          if (str._1.nonEmpty && str._2.nonEmpty)
//            (str._1 + ", " + name, str._2 + ", " + "?")
//          else (name, "?")
//      )
//      val insertSQL = s"insert into my_table ($fieldNames) values ($placeholders)"
//      df.foreachPartition(records => {
//        try {
//          var count = 0;
//          val chDatasource = new ClickHouseDataSource(chUrl, pro)
//          val chConn = chDatasource.getConnection("default", "123456")
//          val psmt = chConn.prepareStatement(insertSQL)
//          while (records.hasNext) {
//            val record = records.next()
//            var fieldIndex = 1
//            record.schema.fields.foreach(field => {
//              // 匹配类型，获取对应类型的字段值
//              field.dataType match {
//                case StringType =>
//                  psmt.setString(fieldIndex, record.getAs[String](field.name))
//                case LongType =>
//                  psmt.setLong(fieldIndex, record.getAs[Long](field.name))
//                case DoubleType =>
//                  psmt.setDouble(fieldIndex, record.getAs[Double](field.name))
//                // 这里可以新增自己需要的type
//                case _ => println(s" other type: ${field.dataType}")
//              }
//              fieldIndex += 1
//            })
//            psmt.addBatch()
//            // 批量写入
//            if (count % batchSize == 0) {
//              psmt.executeBatch()
//              psmt.clearBatch()
//            }
//            count += 1
//          }
//          psmt.executeBatch()
//          psmt.close()
//          chConn.close()
//        } catch {
//          case e: Exception =>
//            e.printStackTrace()
//        }
//      })
//      chShardIndex += 1
//    }
//    spark.close()
//  }
//}
