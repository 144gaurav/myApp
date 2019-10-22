package com.demo.rdd

import java.util

import scala.collection.JavaConversions._
import org.apache.spark._
import org.apache.spark.sql._

import org.apache.spark.{SparkConf, SparkContext}

object AvgMovieRatings extends App {

  val conf = new SparkConf().setAppName("Min Temp").setMaster("local[*]")
  val sc = new SparkContext(conf)
sc.setLogLevel("WARN")
  val baseRdd = sc.textFile("/home/gaurav/Sample_Data/ml-100k/u.data")
  val rdd = sc.textFile("/home/gaurav/Sample_Data/ml-100k/u.data").map(_.split("\t")).map(x=>(x(1),x(2).toInt))
  val avgRatingRdd = rdd.groupByKey().map(x=>(x._1,x._2.sum/x._2.size))

 // val projRDD = baseRdd.map(getMovieIdAndScore)
  println(s"No. of default is : ${baseRdd.getNumPartitions}");
  val projRDD = baseRdd.mapPartitions(getMovieIdAndScoreMapPartition)


  println("7&&&&&&&&&&&&&&&&&&&&&&&&")
  projRDD.collect()
  val validRDD = projRDD.filter(_.isLeft).map(_.left.get)
  val invalidRdd = projRDD.filter(r=>r.isRight).map(r=>r.right.get)
  val ratingsRDD = validRDD.map(r=>{
    val rating = r._2*2
    val movie = r._1
    (movie,rating)
  })

  val df = ratingsRDD

  ratingsRDD.foreach(println)


  def getMovieIdAndScore(record:String) : Either[(String,Double),String]=
    {
      val cols = record.split("\\t")

      try{
        Left(cols(1),cols(2).toDouble)
      }
catch
  {
    case _:Exception=>Right(record)

  }
    }

  def getMovieIdAndScoreMapPartition(records:Iterator[String]) : Iterator[Either[(String,Double),String]]=
  {
    println("INside Map Partitions")
val outRecords =  records.map(record=>{
      val cols = record.split("\\t")
      try{
       Left(cols(1),cols(2).toDouble)
      }
      catch
        {
          case _:Exception=>Right(record)

        }
    })
    outRecords
  }

}
