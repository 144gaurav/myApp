package com.demo.rdd

import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import org.apache.spark.sql.functions._



object PopularSuperHeroDataSet extends App {
  case class SuperHero(id:Int,count:Int)
  def countCoOccurences(line: String) = {
    var elements = line.split("\\s+")
    ( elements(0).toInt, elements.length - 1 )
  }
  
  // Function to extract hero ID -> hero name tuples (or None in case of failure)
  def parseNames(line: String) : Option[(Int, String)] = {
    var fields = line.split('\"')
    if (fields.length > 1) {
      return Some(fields(0).trim().toInt, fields(1))
    } else {
      return None // flatmap will just discard None results, and extract data from Some results.
    }
  }
  
  val session = SparkSession.builder()
  .appName("Popular Superhero")
  .master("local[*]")
  .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
  .getOrCreate()
  
   val names = session.sparkContext.textFile("../marvel-names.txt")
   // val namesRdd = names.map(parseNames).filter(_.nonEmpty).map(_.get)
        val namesRdd = names.flatMap(parseNames)

    //namesRdd.foreach(println)
    // Load up the superhero co-apperarance data
    val lines = session.sparkContext.textFile("../marvel-graph.txt")
    
    // Convert to (heroID, number of connections) RDD
    val pairings = lines.map(countCoOccurences).map(x=>SuperHero(x._1,x._2))
    
    import session.implicits._
    val superHeroDataSet = pairings.toDF()
    
    val xx = superHeroDataSet.groupBy("id").agg(sum("count").alias("sum")) .orderBy(desc("sum"))
    xx.show()
//xx.agg(max("sum(count)")).show()

  
  
}