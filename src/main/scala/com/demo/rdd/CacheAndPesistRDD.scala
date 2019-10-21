package com.demo.rdd

import org.apache.hadoop.hdfs.server.common.Storage
import org.apache.spark.{SparkConf, SparkContext}

object CacheAndPesistRDD extends App {
  val conf = new SparkConf().setAppName("Min Temp").setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  val baseRdd = sc.textFile("/home/gaurav/Sample_Data/ml-100k/u.data")
baseRdd.cache()



}
