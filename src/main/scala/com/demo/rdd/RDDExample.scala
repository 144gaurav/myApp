package com.demo.rdd

import org.apache.spark.{SparkConf, SparkContext}

object RDDExample extends App {
  val conf = new SparkConf().setAppName("Min Temp").setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  val baseRdd = sc.textFile("/home/gaurav/Sample_Data/ml-100k/u.data")

}
