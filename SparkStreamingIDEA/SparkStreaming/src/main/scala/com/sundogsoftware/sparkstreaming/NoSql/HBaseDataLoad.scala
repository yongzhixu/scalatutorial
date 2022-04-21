package com.sundogsoftware.sparkstreaming.NoSql

import java.util.UUID

object HBaseDataLoad {

  def main(args: Array[String]): Unit = {
    //    You can insert data into Hbase using the add() method of the Put class.
    //    All of these classes belong to the org.apache.hadoop.hbase.client package.
    //    See the scala program below to create data in “employee” Table of HBase.
    import org.apache.hadoop.hbase.HBaseConfiguration

    val hBaseConf = HBaseConfiguration.create()
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

    //**********************************Insert Record into HBase Table********************************//
    import org.apache.hadoop.hbase.TableName
    val tableName = "employee"
    val table = TableName.valueOf(tableName)
    val HbaseTable = conn.getTable(table)

    //    set the column Families
    val cPersonal = "personal"
    val cProfessional = "professional"

    //    build list of records to insert
    val records: List[Map[String, Any]] = List(
      Map(
        "id" -> UUID.randomUUID().toString,
        "name" -> "Raju Karappan",
        "city" -> "St.Augustine",
        "designation" -> "Sr.Technique Architect",
        "salary" -> Math.ceil(Math.random()*100)),
      Map(
        "id" -> UUID.randomUUID().toString,
        "name" -> "Raju Karappan",
        "city" -> "St.Augustine",
        "designation" -> "Sr.Technique Architect",
        "salary" -> Math.ceil(Math.random()*100)),
      Map(
        "id" -> UUID.randomUUID().toString,
        "name" -> "Raju Karappan",
        "city" -> "St.Augustine",
        "designation" -> "Sr.Technique Architect",
        "salary" -> Math.ceil(Math.random()*100))
    )

    //    Iterate through each record to insert
    for (row <- records) {
      val keyValue = "Key_" + row.getOrElse("id", "NULL")
      val transRec = new Put(Bytes.toBytes(keyValue))
      val name = row.getOrElse("name", "NULL").toString
      val city = row.getOrElse("city", "NULL").toString
      val salary = row.getOrElse("salary", "NULL").toString
      val designation = row.getOrElse("designation", "NULL").toString

      //**************Add specified column and value, with the specified timestamp as its version to this put operation*******//

      //      add name and city to the personal column
      transRec.addColumn(Bytes.toBytes(cPersonal), Bytes.toBytes("name"), Bytes.toBytes(name))
      transRec.addColumn(Bytes.toBytes(cPersonal), Bytes.toBytes("city"), Bytes.toBytes(city))

      //      add designation and salary to the professional column
      transRec.addColumn(Bytes.toBytes(cProfessional), Bytes.toBytes("designation"), Bytes.toBytes(designation))
      transRec.addColumn(Bytes.toBytes(cProfessional), Bytes.toBytes("salary"), Bytes.toBytes(salary))

      //      Insert record into HBase
      HbaseTable.put(transRec)

    }
    HbaseTable.close()
    conn.close()
  }
}
