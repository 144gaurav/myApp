package com.demo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import scala.io.Source

object ReadWriteFiles extends App{

  val conf = new SparkConf()
  val spark = SparkSession
              .builder
    .appName("Demo App")
    .master("local[*]")
    .config("spark.drgynamic.allocation","true")
   // .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._
case class sale (top:String,total:Long,year:Int)

  val newSchema = StructType(
    List(
      StructField("top", StringType, true),
      StructField("total", LongType, true),
      StructField("year", IntegerType, true)
    )
  )

  println()

  /**
   * Read from parquet file
   */
  val df = spark.read.format("parquet")
    .option("inferSchema",true).load("/home/gaurav/Downloads/extra/parquet")
  println(df.first())
//df.map(x=> x.get(1).toString.toInt+2).show()
  //df.filter("sadasda==asfc")
  val ds= df.as[sale]
ds.filter(x=>x.year>30)
  val dfRDD = df.rdd
  val dsRDD = ds.rdd
val df22 = df.columns

  dsRDD.map(x=>x.year+1)
  ds.map(x=>x.year)
  val dataSet1 = ds.map(x=>sale(x.top,x.total*2,x.year))
  dataSet1.show(false)
  val df1 = spark.read.format("json")
    .option("inferSchema",true).load("/home/gaurav/Downloads/extra/json")
  df.printSchema()
  df1.printSchema()
  df1.show(50)
  /**
   * Save in json file
   */
//df.write.partitionBy("year").format("json").save("/home/gaurav/Downloads/extra/buc")
  /**
   * Save in hive table
   */
  //df.write.mode("append").saveAsTable("dbname.tablename")
  val dd = Source.fromFile("/home/gaurav/Downloads/extra/Product.csv").getLines()
  val line1 = dd.next()
  println(line1)

}
