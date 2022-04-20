package com.sundogsoftware.sparkstreaming.NoSql
// https://creativedata.atlassian.net/wiki/spaces/SAP/pages/54067227/Spark+Scala+-+Read+Write+files+from+Hive
import com.sundogsoftware.sparkstreaming.HelloWorld
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object HiveDBRead {

  case class HelloWorld(message: String)

  def main(args: Array[String]): Unit = {


    val logger = LogManager.getRootLogger
    val hdfs_master = "hdfs://192.168.50.202:8020/"
    //    Step 1 Create Spark Session
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)
//      .set("hive.metastore.warehouse.dir",hdfs_master+"apps/hive/warehouse")
      .set("spark.sql.warehouse.dir",hdfs_master+"apps/hive/warehouse/foodmart")

    val sparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    import sparkSession.implicits._

    // ======= Writing files
    // Writing Dataframe as a Hive table
    import sparkSession.sql

    sql("DROP TABLE IF EXISTS helloworld")
    sql("CREATE TABLE helloworld (message STRING)")

    val df = Seq(HelloWorld("helloworld")).toDF().coalesce(1)
    df.write.mode(SaveMode.Overwrite).saveAsTable("helloworld")
    logger.info("Writing hive table : OK")

    import sparkSession.sql
    // ======= Reading files
    // Reading hive table into a Spark Dataframe
    val dfHive = sql("SELECT * from helloworld")
//    val dfHive = sql("show databases")
    logger.info("Reading hive table : OK")
    logger.info(dfHive.show())
    logger.info("Reading hive table account: OK")
    sql("SELECT * from account").show()
  }

}
