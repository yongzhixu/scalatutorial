//import org.apache.kudu.spark.kudu._
//
//// Create a DataFrame that points to the Kudu table we want to query.
//val df = spark.read.options(Map("kudu.master" -> "kudu.master:7051",
//  "kudu.table" -> "default.my_table")).format("kudu").load
//// Create a view from the DataFrame to make it accessible from Spark SQL.
//df.createOrReplaceTempView("my_table")
//// Now we can run Spark SQL queries against our view of the Kudu table.
//spark.sql("select * from my_table").show()