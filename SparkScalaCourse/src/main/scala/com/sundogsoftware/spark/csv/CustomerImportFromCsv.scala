package com.sundogsoftware.spark.csv

object CustomerImportFromCsv {

  def main(args: Array[String]): Unit = {

    //    val filepath = ""
    //    import spark.implicits._
    //    spark.read.csv(s"${filepath}_withId").createOrReplaceTempView("customer_import")
    //
    //    val customerDF_mysql = spark.sql("select 1 as tenant_id, cast (0 as bigint) as version, cast(id + 500000000000000000 as bigint) as id, wechat_fans_name as nick_name," +
    //      "now() as date_created,now() as last_updated," +
    //      "now() as date_join,'ImportFromExternalSystem' as create_method from customer_import").toDF()
    //    customerDF_mysql.write.mode("append")
    //      .format("jdbc")
    //      .option("driver", "com.mysql.jdbc.Driver")
    //      .option("url", "jdbc:mysql://192.168.123.205:3306/xiaoshu?rewriteBatchedStatements=true")
    //      .option("dbtable", "customer") //表名
    //      .option("user", "root")
    //      .option("password", "")
    //      .save()
  }

}
