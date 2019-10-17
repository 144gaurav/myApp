package com.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object SparkObject1 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .master("local")
    .getOrCreate()

  import spark.implicits._
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

  val df1 = df.filter($"_corrupt_record".isNull)
  df1.show(false)
  val df2 = df.filter($"_corrupt_record".isNotNull).select("_corrupt_record").map(row=>row.getString(0).split(",")).collectAsList()
  val df3 = df.filter($"_corrupt_record".isNotNull).select("_corrupt_record")
  df3.write.mode("append").format("text").save("file:///C:/SparkScala/corrupt")

  for(i<- 0 until df2.size())
  {
    for (x <- df2.get(i))
    {
      println("Changes in Dev Branch "+x)
    }
  }

}
