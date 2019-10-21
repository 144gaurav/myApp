package com.demo.rdd


import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object newHadoopRDD extends App {

  val conf = new SparkConf().setAppName("Min Temp").setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  val rdd = sc.newAPIHadoopFile("/home/gaurav/Sample Data/1800.csv",classOf[TextInputFormat],classOf[LongWritable],classOf[Text],sc.hadoopConfiguration).map(r=> (r._1.get(),r._2.toString))
  println(rdd.first)

}
