package com.demo.rdd

import org.apache.log4j._
import org.apache.spark.{SparkConf, SparkContext}

//import scala.tools.cmd.Spec.Accumulator

object MinTemp extends App{

  val conf = new SparkConf().setAppName("Min Temp").setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  val rdd = sc.textFile("/home/gaurav/Sample_Data/1800.csv").map(_.split(',')).map(x=>(x(0),x(2),Math.round(((x(3).toFloat)-32) *.055)))
val filterRdd = rdd.filter(_._2=="TMIN").map(x=>(x._1,x._3)).groupByKey()

  val minTem = rdd.filter(_._2=="TMIN").map(x=>(x._1,x._3)).reduceByKey((x,y)=>{if(x>y) y else x})
println("************")
  minTem.foreach(println)
  println("************")
  val minTempRdd = filterRdd.map(x=>(x._1,x._2.min))
  minTempRdd.foreach(println)
  val avgTempRdd = rdd.map(x=>(x._1,(x._3,1))).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
  val avgTempFinal = avgTempRdd.map(x=>(x._1,x._2._1/x._2._2))
  avgTempFinal.foreach(println)
  val tt = rdd.map(x=>(x._1,x._3)).groupByKey()
  val cc1 = tt.map(x=>(x._1,(x._2.sum/x._2.size)))
  //cc1.foreach(println)

  /**
   * Aggregate by key Example
   */
  val filterRdd1 = rdd.filter(_._2=="TMIN").map(x=>(x._1,x._3.toFloat))
  val initialValue:Float = 0
  def sequenceFunc (accumulator:Float , value:Float) = {
    if(accumulator<value)
      accumulator
    else
      value
  }

  def combinerFunc(accumulator1: Float,accumulator2:Float): Float =
  {
    if(accumulator1<accumulator2)
      accumulator1
    else
      accumulator2
  }
println("************")
  val aggRdd = filterRdd1.aggregateByKey(initialValue)(sequenceFunc,combinerFunc)
aggRdd.foreach(println)

}
