package com.hortonworks.scythe

import java.text.SimpleDateFormat

import org.junit.Assert
import org.junit.Before
import org.junit.Test

import breeze.interpolation.LinearInterpolator
import breeze.linalg.DenseVector
import com.hortonworks.scythe.functions._
import org.apache.spark.sql.SparkSession

class FunctionsTest {

 
  @Before def initialize() {
    println("Function Tests")
  }
  
  /*
  def average[T]( elems: Iterable[T] )( implicit num: Numeric[T] ) = {
    num.toDouble( elems.sum ) / elems.size
  }
  
  implicit def iterebleWithAvg[T:Numeric](data:Iterable[T]) = new {
    def avg = average(data)
  }
  */
  
  @Test def avg() {
    
    
    val l = List(1,2,3,4,5)
    //dfd.movAvg5
    
   val movAvg3 = l.movAvg3
   
   val twa = l.twma
    
    val abc = Array(2,4,5,6)
    abc.toIterable.avg
    
    
    //ScytheUtils.avg(ListSytcheUtils.sliding(abc))
    
   val a =  List(1,2,3,4,5)
   a.avg
   a.movAvg3
   
   val b = List( (1.0, 20000000000L), (2.0, 55000000000L), (3.0, 6900000000L))
   val avg = b.map(x=>x._2).avg
   println(avg)
   //println(f.avg(a.toList))
   
   val c = a.sliding(2, 1) 
   println (a.sliding(2, 1).map { _.avg }.toList)
   
   a.movAvg5
   println (a.movAvg3.toList)
   println (a.movAvg5.toList)
   
   println (b.map(_._2).movAvg3.toList)
   
   
   val spark = org.apache.spark.sql.SparkSession.builder.
      master("local")
      .appName("spark function test")
      .getOrCreate()
      
    import spark.implicits._
      
   val rdd = spark.sparkContext.parallelize(a)
   
   val m = rdd.movAvg3
   //m.foreach { println }
   
   
    //println(rs)
  }

}