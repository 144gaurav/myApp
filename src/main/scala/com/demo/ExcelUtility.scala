package com.demo

import java.io.FileInputStream
import java.util.Properties
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, IntegerType, StringType, StructField, StructType}
import com.crealytics.spark.excel
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._

object ExcelUtility extends App {


  val spark = SparkSession.builder.master("local[*]").getOrCreate()
  val df = spark.read.format("com.crealytics.spark.excel")
    .option("path", "file:///home/gaurav/Sample_Data/sam.xlsx")
    .option("useHeader", "true")
    //.option("treatEmptyValuesAsNulls", "true")
    //.option("inferSchema", "true")
    .load()
  df.show()

  val newSchema = StructType(
    List(
      StructField("Order_item_id", IntegerType, true),
      StructField("order_item_order_id", IntegerType, true),
      StructField("order_item_product_id", IntegerType, true),
      StructField("order_item_quaatity", IntegerType, false),
      StructField("order_item_subtotal", DoubleType, true),
      StructField("order_item_product_price", DoubleType, true)
    )
  )
  val dvDF = spark.read.format("csv").load("/home/gaurav/Sample_Data/").na.fill("0", Seq("_c3")).rdd.map(row => {
    try {
      Left(Row(row.getString(0).toInt, row.getString(1).toInt, row.getString(2).toInt, row.getString(3).toInt, row.getString(4).toDouble, row.getString(5).toDouble))

    } catch {
      case _: Exception => Right(row)

    }
  })
  val df4rdd = dvDF.filter(_.isLeft).map(_.left.get)
  val interDF = spark.createDataFrame(df4rdd, newSchema)
  val finalDF = interDF
    .transform(revenue_col)
    .transform(revenue_status_col)
  finalDF.show(false)
  finalDF.printSchema()

  val dfSchema = finalDF.dtypes
  val colname = "ColumnName"
  val colType = "Product"

  val filDF = df.select(colname, colType).rdd.map(x => (x.get(0).toString, x.get(1).toString)).collect().toMap
  filDF.foreach(println)

  val props: Properties = new Properties
  props.load(new FileInputStream("/home/gaurav/Sample_Data/sam.properties"))
  println(props.getProperty("bintray_apikey"))



  def revenue_col(df: DataFrame): DataFrame = {
    df.withColumn("revenue", round(df.col("order_item_subtotal") * df.col("order_item_product_price"), 2))
  }

  def revenue_status_col(df: DataFrame): DataFrame = {
    df.withColumn("sale_status", when(df.col("revenue") > 5000, "HIGH").otherwise("LOW"))
  }
}
