package com.sundogsoftware.spark.kudu

import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.{Schema, Type}
import org.apache.kudu.client.{CreateTableOptions, KuduClient, KuduTable}

import scala.collection.JavaConverters._
import scala.util.Try

class KuduTableCreate(client: KuduClient) {

  private val cols = Seq(
    new ColumnSchemaBuilder("id", Type.INT64).key(true).build(),
    new ColumnSchemaBuilder("title", Type.STRING).build(),
    new ColumnSchemaBuilder("subtitle", Type.STRING).nullable(true).build(),
    new ColumnSchemaBuilder("releaseDate", Type.STRING).build(),
    new ColumnSchemaBuilder("director", Type.STRING).build()
  )

  def createMovieTable(name: String): Try[KuduTable] = {

    val schema = new Schema(cols.asJava)
    val options = new CreateTableOptions
    options.addHashPartitions(List("id").asJava, 2)
    Try(client.createTable(name, schema, options))
  }
}
