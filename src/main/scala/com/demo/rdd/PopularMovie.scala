package com.demo.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j._
import org.apache.spark.HashPartitioner
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat


object PopularMovie extends App{
  
  val sc = new SparkContext("local[*]","Popular Movie")
      val lines = sc.textFile("../ml-100k/u.data")
val rdd = lines.map(x=> {
  val xx = x.split("\t")
  (xx(1),xx(2).toInt)
})
//rdd.foreach(println)
    Logger.getLogger("org").setLevel(Level.ERROR)

val popularMov = rdd.groupByKey().map(x=>(x._1,x._2.size))
val sortedRdd = popularMov

val moviesNameRDD = sc.textFile("../ml-100k/u.item")
moviesNameRDD.coalesce(1000)
val moviename = moviesNameRDD.map(x=>{val xx = x.split("\\|")
  (xx(0),xx(1))
  }).partitionBy(new HashPartitioner(100)).collect().toMap
//  val moviename2 = moviesNameRDD.map(x=>{val xx = x.split("\\|")
//  (xx(0),xx(1))
//  }).partitionBy(new HashPartitioner(10))
  
  val nameDic = sc.broadcast(moviename)
  
  //val xx= sortedRdd.join(moviename2).map(x=>(x._2._2,x._2._1.toInt)).sortBy(_._2, false)
  val xx = sortedRdd.map(x=>(nameDic.value(x._1),x._2.toInt)).map(x=>(x._2,x._1)).sortByKey(false)
  xx.saveAsNewAPIHadoopFile("c:/demo1", classOf[LongWritable], classOf[Text], classOf[TextOutputFormat[LongWritable,Text]], sc.hadoopConfiguration)
  
 // val named = sc.broadcast(moviename)
  //val movieNameRDD = sortedRdd.map(x=>(named.value(x._1),x._2)).collect()
  xx.foreach(println)
  
 // val rddzipped = moviename.map(x=> (x._1,x._2))
  
  
  
//val countRDD = rdd.countByKey().toList.sortBy(_._2)
//countRDD.foreach(println)
  
  
}