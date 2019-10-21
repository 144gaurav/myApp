package com.demo.rdd

import org.apache.spark._
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import scala.collection.immutable.HashMap
import scala.io.Source

object MostPopularSuperHero extends App{
  val conf = new SparkConf().setAppName("Min Temp").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val rdd =  sc.textFile("../Marvel-graph.txt").map(cleanData)
  val reduceRDD = rdd.reduceByKey((x,y)=>(x+y)).map(x=>(x._2,x._1.toInt)).max() //.sortByKey(false).coalesce(1).take(1)
 // reduceRDD.foreach(println)
 val xx = sc.broadcast(getGraphData())
  val xx1 =0
  val acc = sc.accumulator(xx1)
println("Most Popular SuperHero is :" +xx.value(reduceRDD._2) + "  " + reduceRDD._1)
val xxx1 = getGraphData()
  println(xxx1.get(reduceRDD._2))



  def cleanData(data:String)={
    val splitData = data.split(" ")
    (splitData(0),splitData.length-1)

  }

  def getGraphData():HashMap[Int,String]={
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    var mapData:HashMap[Int,String] = HashMap()
    val data = Source.fromFile("../Marvel-names.txt").getLines()
 //data.foreach(println)
    for (x <- data)
    {var value = x.split('\"')
    if(value.length>1)
    mapData += (value(0).trim.toInt -> value(1))
    }
    mapData
  }


}

