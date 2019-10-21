package com.demo.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._

case class Demo (vehicle_id:String,date:String)

object SparkCassandra extends App {

  val conf = new SparkConf().setAppName("Cassandra APp").setMaster("local[*]")
  conf.set("spark.cassandra.connection.host","localhost")
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext(conf)
  val cassyRDD = sc.cassandraTable("home_security","location")
  cassyRDD.foreach(println)
  println(cassyRDD.getNumPartitions)

  val demoRDD = cassyRDD.map(x=>{
    val id = x.getString("vehicle_id")
    val date = x.getString("date")
    val demoTab = Demo(id,date)
  demoTab
  })

  demoRDD.saveToCassandra( "home_security","demo")
}
