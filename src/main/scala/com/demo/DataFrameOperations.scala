package com.demo

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import scala.io.Source

object DataFrameOperations extends App {

  val sparkSession = SparkSession.
    builder().
    master("local[*]").
    appName("Data Frame Example").
    getOrCreate()


//  val spark = SparkSession
//    .builder()
//    .master("local[*]")
//    .config("spark.executor.memory", "70g")
//    .config("spark.driver.memory", "50g")
//    .config("spark.memory.offHeap.enabled",true)
//    .config("spark.memory.offHeap.size","16g")
//    .appName("sampleCodeForReference")
//    .getOrCreate()

  import sparkSession.implicits._
  //"OrderId","Date","Price","Status"

  /*
  Convert the text to Data type columns with alias
   */
  /**
   * Create Data frame with Data Types. Process 1
   */
  val orderTypeDF = sparkSession.read.text("/home/gaurav/Sample_Data/orders.csv").toDF()


  val orderDF = orderTypeDF.select(split($"value", ",")(0).cast("int").alias("OrderId")
    , split($"value", ",")(1).cast("timestamp").alias("Date"),
    split($"value", ",")(2).cast("int").alias("Price"),
    split($"value", ",")(3).cast("string").alias("Status"))
  orderDF.printSchema()

  /**
   * Create Data frame with Data Types from RDD. Process 2
   */

  val orderitemRDD = sparkSession.sparkContext.textFile("/home/gaurav/Sample_Data/orders_items.csv").map(x => {
    val value = x.split(",")
    (value(0).toInt, value(1).toInt, value(2).toInt, value(3).toInt, value(4).toDouble, value(5).toDouble)
  })


  /**
   * Data Validations Step
   */
  val orderitemRDD1 = sparkSession.sparkContext.textFile("/home/gaurav/Sample_Data/orders_items.csv").map(x => {
    val value = x.split(",")
    try {
      Left(Row(value(0).toInt, value(1).toInt, value(2).toInt, value(3).toInt, value(4).toDouble, value(5).toDouble, false))

    } catch {
      case _: Exception => Right(x)

    }
  }).filter(_.isLeft).map(_.left.get)


  def dataType(type1: String) = {
    type1 match {
      case "Int" => IntegerType
      case "Double" => DoubleType
      case "String" => StringType
    }


  }

  val test = Source.fromFile("/home/gaurav/file").getLines()
  var arrayFields: Array[String] = Array()
  for (x <- test) {
    arrayFields = x.split(",")

  }


  var arrayFields2 = arrayFields.map(x => {
    StructField(x.split(":")(0), dataType(x.split(":")(1)))
  })

  val field = StructField("QuantityCheck", BooleanType, true)
  val newArray = arrayFields2 :+ field
  newArray.foreach(println)

  var arra1: Array[String] = Array()

  arra1.foreach(println)

  val newSchema = StructType(
    List(
      StructField("Order_item_id", IntegerType, true),
      StructField("order_item_order_id", IntegerType, true),
      StructField("order_item_product_id", IntegerType, true),
      StructField("order_item_quantity", IntegerType, true),
      StructField("order_item_subtotal", DoubleType, true),
      StructField("order_item_product_price", DoubleType, true),
      StructField("QuantityCheck", BooleanType, true)
    )
  )
  val newSchema1 = StructType(
    List(
      StructField("Order_item_id", IntegerType, true),
      StructField("order_item_order_id", IntegerType, true),
      StructField("order_item_product_id", IntegerType, true),
      StructField("order_item_quantity", IntegerType, true),
      StructField("order_item_subtotal", DoubleType, true)
    )
  )
  val ordersDFNew = sparkSession.createDataFrame(orderitemRDD1, newSchema)



  println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
  ordersDFNew.printSchema()


  /**
   * User Defined Functionsd
   */
  val quantityCheck = (x: Int) => {
    if (x < 2) false
    else
      true
  }

  val quantityCheckUDF = udf(quantityCheck)

  val tempDF = ordersDFNew.withColumn("QuantityCheck", quantityCheckUDF($"order_item_quantity"))//.as("String")
  val finalFilteredDF = tempDF.where($"QuantityCheck" === true).drop($"QuantityCheck")
  val deleteDuplicateDF = finalFilteredDF.dropDuplicates().orderBy($"Order_item_id")
  deleteDuplicateDF.show(false)
  println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")


  val orderItemsDF = orderitemRDD.toDF("Order_item_id", "order_item_order_id", "order_item_product_id", "order_item_quantity", "order_item_subtotal", "order_item_product_price")

//  orderItemsDF.write.partitionBy("key").insertInto("sdfgs")
  val featureDF = orderItemsDF.withColumn("Order_item_id", orderItemsDF.col("Order_item_id").cast("int"))
  featureDF.printSchema()
  //orderDF.printSchema()
  // orderDF.show()
  val interDF = orderDF.select($"OrderId".cast("int"), $"Date", lower($"Status").alias("Status")).withColumn("Current_Date", current_date())
  val interDF1 = orderDF.selectExpr("OrderId", "lower(Status)", "current_timestamp as Current_Date")
  val filterOrderedDF = orderDF.selectExpr("case when Status ='' then 'demo' else Status end as Status").where("Status ='demo'")

  //filterOrderedDF.show()
  /**
   * Write Data Frame in Files
   */
  //interDF1.write.format("csv").save("/home/gaurav/Sample_Data/01")
  // interDF1.write.csv("/home/gaurav/Sample_Data/02")
  sparkSession.conf.set("spark.sql.shuffle.partitions", 2)
  // interDF.groupBy($"Status").agg(count("OrderId")).write.format("csv").save("/home/gaurav/Sample_Data/01")
  // where(sampleDF.col("Status")==="CLOSED" or sampleDF.col("Status")==="PENDING").show()
  // where(orderDF.col("Status").isin("PENDING","CLOSED") and orderDF.col("Date").like("2013-08%")).show()

  val ff = interDF.agg(count("OrderId")).groupBy($"Status")

  interDF.distinct().where($"OrderId".isin(1, 2)).show(false)

  //interDF.join(broadcast(interDF1),"left")
//interDF.filter(interDF("")==="" and interDF("")==="")

  //orderItemsDF.printSchema()

  //orderItemsDF.where("order_item_subtotal != round((order_item_quantity*order_item_product_price),2)").show()
  // orderItemsDF.where(orderItemsDF.col("order_item_subtotal") !==   (orderItemsDF.col("order_item_quantity") * orderItemsDF.col("order_item_product_price"))).show()
  //orderDF.where("date_format(Date,'dd') = '01'").show()
  /**
   * User Defined Functionsd
   */
  val udffff = (x: String) => {
    x.split("-")(0).toInt
  }

  val udfDF = udf(udffff)

  //orderDF.select(udfDF($"Date")).show(false)


  /**
   * Joins in Data Frames
   */

  //orderDF.join(orderItemsDF,orderDF.col("OrderId") === orderItemsDF.col("order_item_order_id"))
  //orderDF.join(orderItemsDF,orderDF.col("OrderId") === orderItemsDF.col("order_item_order_id"),"left").
  //where(orderItemsDF.col("order_item_order_id").isNull).show()

  //orderDF.groupBy("Status").count().show()
  //orderDF.groupBy(orderDF("Status")).agg(count($"Status").alias("Total Count")).show()

  // orderDF.where($"Status".isin("COMPLETE","CLOSED")).join(orderItemsDF,$"OrderId"===$"order_item_order_id").
  //groupBy($"Date",$"order_item_product_id").agg(sum("order_item_subtotal")).alias("revenue").show()

  /**
   * Register as a temp Table for Running SQL queries on Data Frame
   */
  interDF.createOrReplaceTempView("orders")
  val valueSpark = sparkSession.sql("select * from orders") //.show(false)
  //interDF.write.mode(saveMode = "Append").insertInto("HiveTable")

  /**
   * Windowing Functions
   */


  // orderItemsDF.show(false)
  val specAgg = Window.partitionBy($"order_item_order_id")
  /**
   * Aggregations
   */
  //orderItemsDF.withColumn("Avg Subtotal",avg("order_item_subtotal").over(specAgg)).show(false)

  /**
   * Rank
   */

  // val specRank = Window.partitionBy($"order_item_order_id").orderBy($"order_item_product_price" desc,$"Order_item_id" )
  // orderItemsDF.withColumn("Rank",rank().over(specRank)).write.json("/home/gaurav/Sample_Data/04")

  /**
   * Lead and Lag Windowing Functions
   */
  // val winRank = Window.partitionBy($"order_item_order_id").orderBy($"order_item_product_price",$"Order_item_id" )
  //orderItemsDF.withColumn("Lag",lag($"order_item_product_price",1).over(winRank)).write.json("/home/gaurav/Sample_Data/05")


  /** *
   * Register Multiple Tables in Spark from jdbc
   */
//DataFrameHelpers.validateSchema(ordersDFNew,newSchema)
  val s1:Array[(Tuple2[String,String])] = ordersDFNew.dtypes
  val s2 = newSchema.map(x=>(x.name,x.dataType.toString)).toMap
  var flag = false

  s1.foreach(println)
  s2.foreach(println)
for((a,b)<-s1)
  {
    if(s2.contains(a))
      {
        println("inside IF")
        if (s2.get(a).get.equals(b)) {
          flag = true
          println("Matching")

        }
          }
    if(flag == false)
      {
        println("Not Matching")
      }

  }

//  val url = "jdbc:mysql://127.0.0.1:3306/Firm42"
//  val tables = List("company", "employee", "company_employee")
//
//  val dfs = for {
//   table <- tables
//  } yield (table, sparkSession.read.jdbc(url, table, props))
//
//  for {
//   (name, df) <- dfs
// } df.registerTempTable(name)
}
