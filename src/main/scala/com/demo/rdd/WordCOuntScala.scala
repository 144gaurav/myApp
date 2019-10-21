package com.demo.rdd

import org.apache.spark.{SparkConf, SparkContext}

object WordCOuntScala extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("Word COunt App")
  val sc = new SparkContext(conf)
  val rdd = sc.textFile("/home/gaurav/Sample Data/book.txt").flatMap(x=>x.split("\\W+")).map(_.toLowerCase)
  val countRdd = rdd.countByValue().toList.sortBy(_._2)
  println(countRdd)

  val countRdd1 = rdd.map(x=>(x,1)).reduceByKey((x,y)=>(x+y)).sortBy(x=>x._2,false).collect()


  //countRdd1.foreach(println)
  val maxWord = countRdd.map(x=>(x._2,x._1)).max
  println(maxWord)
}
