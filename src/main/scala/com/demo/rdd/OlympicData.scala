package com.demo.rdd
import org.apache.spark.{SparkConf, SparkContext}

object OlympicData extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("Word COunt App")
  val sc = new SparkContext(conf)
  val rdd = sc.textFile("/home/gaurav/Sample Data/olympix_data.csv").map(x=>x.split("\t")).cache()
  sc.setLogLevel("WARN")  /**
   * Find the total number of medals won by each country in swimming.
   */

  val rdd1 = rdd.map(x=>(x(2),x(3),x(5),x(9).toInt)).filter(_._3=="Swimming")
  val finalRdd = rdd1.map(x=>(x._1,x._4)).reduceByKey((x,y)=>x+y).map(x=>(x,"Swimming"))
  finalRdd.foreach(println)

  /**
   * Find the number of medals that India won year wise.
   */


  val indiaRDD = rdd.map(x=>(x(2),x(3).toInt,x(9).toInt)).filter(_._1=="India").map(x=>(x._2,x._3)).reduceByKey(_+_).map(x=>("India",x))
  indiaRDD.foreach(println)

  /**
   * Find the total number of medals won by each country.
   */

  val totalMedalRdd = rdd.map(x=>(x(2),x(9).toInt)).reduceByKey(_+_).collect().toSeq.sortBy(x=>x._1)
  totalMedalRdd.foreach(println)


}
