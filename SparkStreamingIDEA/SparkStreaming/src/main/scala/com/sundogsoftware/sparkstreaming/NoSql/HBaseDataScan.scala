package com.sundogsoftware.sparkstreaming.NoSql

import org.apache.hadoop.hbase.CompareOperator
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.filter.{BigDecimalComparator, BinaryComparator, ByteArrayComparable, CompareFilter, DependentColumnFilter, FamilyFilter, FilterList, RowFilter, SingleColumnValueExcludeFilter, SingleColumnValueFilter, ValueFilter}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

// value comparator reference" https://github.com/matiji66/hbase-value-comparator/blob/master/src/main/java/test/TestHbaseComparatorJava.java
import java.lang

object HBaseDataScan {
  //  create table employee with two columns: personal and professional.
  //[using HBase shell] create 'employee', 'personal', 'professional'
  //  ref,https://medium.com/@thomaspt748/how-to-create-spark-dataframe-on-hbase-table-e9c8db31bb30
  def main(args: Array[String]): Unit = {
    import org.apache.hadoop.hbase.HBaseConfiguration

    //    Create a spark session with master as “local[*]”
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    //    Set HBase configuration parameters using HBaseConfiguration.create() method.
    val hBaseConf = HBaseConfiguration.create()
    //    hBaseConf.set("hbase.zookeeper.quorum", "localhost")
    hBaseConf.set("hbase.zookeeper.quorum", "192.168.50.202")
    hBaseConf.set("hbase.rootdir", "file:///Uesrs/hadoop/dev/apps/hbase-2.2.2/hbasestorage")
    hBaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hBaseConf.set("zookeeper.znode.parent", "/hbase-unsecure")
    hBaseConf.set("hbase.unsafe.stream.capability.enforce", "false")
    hBaseConf.set("hbase.cluster.distributed", "true")

    //    Create HBase connection using ConnectionFactory.
    import org.apache.hadoop.hbase.client.Connection
    import org.apache.hadoop.hbase.client.ConnectionFactory
    import org.apache.hadoop.hbase.client.Put
    import org.apache.hadoop.hbase.util.Bytes
    val conn = ConnectionFactory.createConnection(hBaseConf)

    //     Lets’ create an HBase table instance.
    import org.apache.hadoop.hbase.TableName
    val tableName = "employee"
    val table = TableName.valueOf(tableName)
    val HbaseTable = conn.getTable(table)

    //     One of the ways to get data from HBase is to scan.
    //     Scan allows iteration over multiple rows for specified attributes.
    //     Lets’ initiate a client Scan instance and setup a prefixFilter criteria to retrieve the rows beginning with “Key”.
    //     Next let’s add columns to be included in the result set.
    //     The following is an example of a Scan on a Table instance.
    import org.apache.hadoop.hbase.client.Result
    import org.apache.hadoop.hbase.client.ResultScanner
    import org.apache.hadoop.hbase.client.Scan
    import org.apache.hadoop.hbase.filter.PrefixFilter
    import org.apache.hadoop.hbase.filter.Filter

    val scan = new Scan()

    val prfxValue = "Key"
    val filterList = new FilterList()
    val prefixFilter: Filter = new PrefixFilter(Bytes.toBytes(prfxValue))

    //    val rowFilter: Filter = new RowFilter()
    val columnFilter: Filter = new SingleColumnValueFilter(Bytes.toBytes("professional"), Bytes.toBytes("salary"),
      CompareOperator.GREATER, new BinaryComparator(Bytes.toBytes("9")))
    val columnExcludeFilter: Filter = new SingleColumnValueExcludeFilter(Bytes.toBytes("professional"), Bytes.toBytes("salary"),
      CompareOperator.GREATER, Bytes.toBytes("8"))
    val valueFilter: Filter = new ValueFilter(CompareOperator.LESS, new BinaryComparator(Bytes.toBytes(99)))
    filterList.addFilter(prefixFilter)
//    filterList.addFilter(valueFilter)
    //    filterList.addFilter(columnExcludeFilter)


    scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"))
    scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("city"))
    scan.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("designation"))
    scan.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("salary"))
    scan.setFilter(filterList)

    val scanner = HbaseTable.getScanner(scan)

//    System.out.println(s"scanner is not empty: ${scanner.iterator().hasNext}")


    //    Iterate through each row and fetch data from each cell and
    //    store the result set in a List[Map[String,String]] collection.

    import org.apache.hadoop.hbase.CellUtil
    var resValues: List[Map[String, String]] = List()
    import scala.collection.JavaConverters._
    for (elem <- scanner.asScala) {
      var resultMap: Map[String, String] = Map()
      val cells = elem.rawCells()
      cells.foreach(cell => {
        val colName = Bytes.toString(CellUtil.cloneQualifier(cell))
        val colValue = Bytes.toString(CellUtil.cloneValue(cell))
        resultMap = resultMap ++ Map(colName -> colValue)
        System.out.print(s"colName is: ${colName},  colValue is: ${colValue};    ")
      })
      System.out.println("")
      val resultList = List(resultMap)
      resValues = resValues ::: resultList
    }

    //    Destroy instances of Scan, Table and Connection to release any resources held..
    scanner.close()
    HbaseTable.close()
    conn.close()

    //    Let’s create a Spark DataFrame using the List[Map[String,String]] collection.
    val colValLstMap = resValues

    System.out.println(s"colValLstMap size is: ${colValLstMap.length}.")
    //    get column name from the map
    val colList = colValLstMap.map(x => {
      x.keySet
    })

    //    get all unique columns from the list
    val uniqColList = colList.reduce((x, y) => x ++ y)
    val emptyString = ""
    //add empty value for the non existing keys
    val newColValMap = colValLstMap.map(eleMap => {
      uniqColList.map((col => {
        (col, eleMap.getOrElse(col, emptyString))
      })).toMap
    })
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.Row

    //    create rows
    val rows = newColValMap.map(m => {
      Row(m.values.toSeq: _*)
    })
    //    create the schema from the header
    val header = newColValMap.head.keys.toList
    val schema = StructType(header.map(fieldName => StructField(fieldName, StringType, true)))
    val sc = spark.sparkContext

    //    create rdd
    val rdd = sc.parallelize(rows)

    //    create dataframe
    val resultDF = spark.sqlContext.createDataFrame(rdd, schema)

    resultDF.sort("salary").show(100, false)
  }
}
