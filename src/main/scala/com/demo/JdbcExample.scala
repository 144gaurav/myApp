package com.demo

import java.io.FileInputStream
import java.sql.Date
import java.util.Properties

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import com.crealytics.spark.excel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import java.util.Properties

import org.apache.spark.sql.expressions._
import sun.awt.AWTAccessor.WindowAccessor

object JdbcExample extends App {

  val spark = SparkSession.builder().master("local[*]")
    .getOrCreate()
  val jdbcHostname = "localhost"
  val jdbcDatabase = "employees"
  val jdbcUrl = s"jdbc:mysql://${jdbcHostname}/${jdbcDatabase}"
  val connectionProperties = new Properties()
  val jdbcUsername = "root"
  val jdbcPassword = "password"
  connectionProperties.put("user", s"${jdbcUsername}")
  connectionProperties.put("password", s"${jdbcPassword}")
  connectionProperties.put("driver", "com.mysql.jdbc.Driver")

  case class emp(salary: Integer, from_date: Date, to_date: Date, emp_no: Integer, first_name: String)

  import spark.implicits._

  val emp_df = spark.read.jdbc(jdbcUrl, "employees", connectionProperties)
  val mgr_df = spark.read.jdbc(jdbcUrl, "dept_manager", connectionProperties)
  val sal_df = spark.read.jdbc(jdbcUrl, "salaries", connectionProperties)

//  val winSpecForIndex = Window.orderBy(emp_df("emp_no"))
//  val indexDFRowNum = emp_df.withColumn("Row_Id", row_number().over(winSpecForIndex))
//  indexDFRowNum.show(false)
//
//  val empIndexSchema = StructType(emp_df.schema.fields ++ Array(StructField("Row_Id", LongType, false)))
//  val emp_df_rdd = emp_df.rdd.zipWithIndex()
//  val indexDF_Rdd = spark.createDataFrame(emp_df_rdd.map { case (row, index) => Row.fromSeq(row.toSeq ++ Array(index)) }, empIndexSchema)
//  //val indexDF_Rdd1 = spark.createDataFrame(emp_df_rdd.map{row=> Row.fromSeq(row._1.toSeq ++ Array(row._2))},empIndexSchema)
//
//  indexDF_Rdd.show(false)

  spark.conf.set("spark.sql.shuffle.partitions", 4)

  val windowSpec = Window.partitionBy($"emp_no").orderBy($"salary" desc)

  val emp_mgr_df = emp_df.join(broadcast(mgr_df), emp_df.col("emp_no") === mgr_df.col("emp_no"))
    .drop(emp_df.col("emp_no"))
    .select("emp_no", "first_name")

  emp_mgr_df.show(false)

  val emp_sal_df = sal_df.join(broadcast(emp_mgr_df), sal_df.col("emp_no") === emp_mgr_df.col("emp_no"))
    .drop(sal_df.col("emp_no")) //.drop(emp_mgr_df.col("from_date"))
  //emp_sal_df.printSchema()

  val window_emp_df = emp_sal_df.withColumn("rank", rank().over(windowSpec))
    .where($"rank" === 1)
    .orderBy($"salary" desc) //.as[emp]
    .cache()

  window_emp_df.show(false)

  //emp_sal_df.createOrReplaceTempView("emp_sal_df")

  //  val window_emp_df = spark.sql("select * from (select *,rank() over (partition by emp_no order by salary desc) as rank from emp_sal_df)a where a.rank =1 order by a.salary desc ").cache()
  //  window_emp_df.show(false)
  val udf_filter_df = window_emp_df.where(date_format(window_emp_df.col("from_date"), "dd").cast(IntegerType) === 28)
    .drop("rank")
    .withColumn("Annual_Salary", annual_sal_udf($"salary"))
    .where($"Annual_Salary" > 1000000).cache()
  //  .filter(row=>row.getAs[Long](5)>1000000)
  window_emp_df.unpersist()
  udf_filter_df.distinct().show(false)
  udf_filter_df.limit(1).show(false)

  /**
   * Adding ROw Id using Zip with Index in DataFrame
   * salary|from_date |to_date   |emp_no|first_name |Annual_Salary|Row_id|
   * +------+----------+----------+------+-----------+-------------+------+
   * |108407|2001-12-28|9999-01-01|110022|Margareta  |1300884      |0     |
   * |103244|2001-12-28|9999-01-01|111400|Arie       |1238928      |1
   */

  val newSchemaIndexed = StructType(udf_filter_df.schema.fields ++ Array(StructField("Row_id", LongType, false)))
  val indexRDD = udf_filter_df.rdd.zipWithIndex()
  val indexedDF = spark.createDataFrame(indexRDD.map { case (row, index) => Row.fromSeq(row.toSeq ++ Array(index)) }, newSchemaIndexed)
  indexedDF.show(false)
  indexedDF.write
    .mode("append")
    .jdbc(jdbcUrl, "new_Table", connectionProperties)

  udf_filter_df.agg(max("salary")).show(false)
  //
  //  val sampleDF = Seq((1,2,"Hello"),((2,3,"world"))).toDF()
  //  sampleDF.show()
  //  sampleDF.printSchema()
  Thread.sleep(5000000)

  def annual_sal_udf = udf((sal: Long) => sal * 12)


}
