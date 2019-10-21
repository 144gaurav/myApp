package com.demo

import java.io.InputStream
import java.lang.reflect.Modifier

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object CorruptRecordDataFrame extends App {
  /**
   * Logger Example
   */

  @transient lazy val logger = Logger.getLogger(getClass.getName)
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .master("local")
    .getOrCreate()
  //spark.conf.set("spark.sql.avro.compression.codec", "snappy")


  import spark.implicits._

  /**
   * Corrupt Report Handling from Struct
   */
  val newSchema = StructType(List(
    StructField("Order_Id",DataTypes.IntegerType , false),
    StructField("Order_Date",DataTypes.TimestampType,false),
    StructField("Quantity",DataTypes.IntegerType,false),
    StructField("Status",DataTypes.StringType,true),
    StructField("_corrupt_record",DataTypes.StringType,true)))

  val df = spark.read
    .format("csv")
    .schema(newSchema)
    .option("inferSchema",true)
    .load("file:///C:/SparkScala/orders.csv").cache()
  df.show(false)
  df.printSchema()

  /**
   * Corrupt record filtering
   */
  val df1 = df.filter($"_corrupt_record".isNull)
  df1.show(false)
  val df2 = df.filter($"_corrupt_record".isNotNull).select("_corrupt_record").map(row=>row.getString(0).split(",")).collectAsList()
  val df3 = df.filter($"_corrupt_record".isNotNull).select("_corrupt_record")
  df3.write.mode("append").format("text").save("file:///C:/SparkScala/corrupt")
val df4 = spark.read.format("com.databricks.spark.avro").load("file:///C:/SparkScala/f1")
  val stream: InputStream = getClass.getResourceAsStream("/orders/orders.csv")
  val lines: Iterator[String] = scala.io.Source.fromInputStream( stream ).getLines
  //lines.foreach(println)

  df4.printSchema()

  for(i<- 0 until df2.size())
  {
    for (x <- df2.get(i))
    {
      logger.info("Insode For Loop")
      println("Hello in Jira 101 Branch "+x)
    }
  }

}
