package com.demo

import java.sql.Struct

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, IntegerType, StringType, StructField, StructType}

object FilterDataFrameExample extends App{

  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.implicits._
  val newSchema = StructType(
    List(
      StructField("Order_item_id", IntegerType, true),
      StructField("order_item_order_id", IntegerType, true),
      StructField("order_item_product_id", IntegerType, true),
      StructField("order_item_quantity", IntegerType, false),
      StructField("order_item_subtotal", DoubleType, true),
      StructField("order_item_product_price", DoubleType, true)
     // StructField("QuantityCheck", BooleanType,false)
    )
  )
val df = spark.read.format("c" +
  "sv")//.schema(newSchema)
  .option("header", "false")
 // .option("inferSchema", "true")
  .load("/home/gaurav/Sample_Data/orders_items.csv")
  //val newDf1 = df.na.drop()
  //val newDf1 = df.na.replace(df.columns,Map("" -> "0000000000"))

//  val map = Map("comment" -> "a", "blank" -> "a2")
//
//  df.na.fill(map).show()

  val newDf1 = df.na.fill("0", Seq("_c3"))
  newDf1.show()
  val newDf = newDf1.filter(row => filterDF(row))
 // df.show(false)
  df.printSchema()
  newDf.show()
val x = "0".toInt

  val dvDF = spark.read.format("csv").load("/home/gaurav/Sample_Data1/").rdd.map(row => {
    try {
      Left(Row(row.getString(0).toInt, row.getString(1).toInt, row.getString(2).toInt, row.getString(3).toInt, row.getString(4).toDouble, row.getString(5).toDouble))

    } catch {
      case _: Exception => Right(row)

    }
  } )

  val df4rdd = dvDF.filter(_.isLeft).map(_.left.get)
  val df4 = spark.createDataFrame(df4rdd,newSchema)
  //df4.persist()
  df4.show()


  def filterDF(row:Row):Boolean ={
   Option(row.get(3)).getOrElse(
     return false)
   true
  }
}
