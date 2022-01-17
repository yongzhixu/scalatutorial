import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.SparkSession


val newOptions: Map[String, String] =
  Map("kudu.table" -> "my_first_table", "kudu.master" -> "192.168.50.203:7051")
// Use new SparkSession interface in Spark 2.0
val spark = SparkSession
  .builder
  .appName("KuduSQL")
  .master("local[*]")
  .getOrCreate()

// Create a DataFrame that points to the Kudu table we want to query.
val df = spark.read.options(Map("kudu.master" -> "192.168.50.203:7051",
  "kudu.table" -> "impala::default.my_first_table")).format("kudu").load
// Create a view from the DataFrame to make it accessible from Spark SQL.
df.createOrReplaceTempView("my_first_table")
// Now we can run Spark SQL queries against our view of the Kudu table.
spark.sql("select * from my_first_table").show()