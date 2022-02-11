package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

/** Find the superhero with the least co-appearances. */
object _7_MostObscureSuperheroDataset {

  case class SuperHeroNames(id: Int, name: String)

  case class SuperHeroOccurrences(value: String)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)


    val sparkSession = SparkSession
      .builder
      .appName("_7_MostObscureSuperheroDataset")
      .master("local[*]")
      .getOrCreate()

    val superHeroNamesSchema = new StructType()
      .add("id", IntegerType, nullable = true)
      .add("name", StringType, nullable = true)

    import sparkSession.implicits._

    val names = sparkSession.read
      .schema(superHeroNamesSchema)
      .option("sep", " ")
      .csv("data/Marvel-names.txt")
      .as[SuperHeroNames]

    val occurrencesLine = sparkSession.read
      .text("data/Marvel-graph.txt")
      .as[SuperHeroOccurrences]

    val occurrences = occurrencesLine
      .withColumn("id", split(col("value"), " ")(0))
      .withColumn("occurs", size(split(col("value"), " ")) - 1)
      .groupBy("id").agg(sum("occurs").alias("occurs"))

    val mostObscure = occurrences
      .filter($"occurs" >= 0)
      .sort($"occurs".asc)
      .first()

    val mostObscureName = names
      .filter($"id" === mostObscure(0))
      .select("name")
      .first()

    println(s"${mostObscureName} ================")
    println(s"${mostObscureName(0)} is the most obscure superhero with ${mostObscure(1)} occurrences ")


    <!-- ============== To find out all name with least occurrence -->

    //find out the least occurrences
    val minConnectionOccurrence = occurrences.agg(min("occurs")).first().getLong(0)
    val mostObscureAll = occurrences
      .filter($"occurs" === minConnectionOccurrence)
      .sort($"occurs".asc)

    val mostObscureAllWithNames = mostObscureAll.join(names, usingColumn = "id")

    println(s"The following characters have only ${minConnectionOccurrence} connection(s):")
    mostObscureAllWithNames.schema.printTreeString()
    mostObscureAllWithNames.select("name").show()
  }
}
