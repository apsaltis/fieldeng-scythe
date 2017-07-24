package com.hortonworks.scythe

import breeze.interpolation._
import breeze.linalg._
import scala.reflect.ClassTag
import scala.Tuple2
import java.util.Date

/**
 * LinearInterpolator is a wrapper of Scala Breeze LinearInterpolator, it provides helper functionality for
 * converting dateTimes to even interval Doubles for interpolation.
 */

class LinearInterpolation {
  
  
  def interpolateDate(s1: List[Tuple2[java.util.Date, Double]], s2: List[Tuple2[java.util.Date, Double]]) :  List[Tuple2[Date, Double]] =  {
    val (ss1, ss2) = date2Double(s1, s2)
    
    val x = DenseVector(ss2.map(x=> x._1):_* )
    val y = DenseVector(ss2.map(y=> y._2):_* )
    
    val f = LinearInterpolator(x,y)
    val ss2yValues = ss1.map{ case (x,y) => f(x) }
    val interpRs = s1.map(x => x._1) zip ss2yValues
    
    return interpRs
  }
  
  def interpolateLong(s1: List[Tuple2[Long, Double]], s2: List[Tuple2[Long, Double]]) :  List[Tuple2[Long, Double]] =  {
    val (ss1, ss2) = long2Double(s1, s2)
    
    val x = DenseVector(ss2.map(x=> x._1):_* )
    val y = DenseVector(ss2.map(y=> y._2):_* )
    
    val f = LinearInterpolator(x,y)
    val ss2yValues = ss1.map{ case (x,y) => f(x) }
    val interpRs = s1.map(x => x._1) zip ss2yValues
    
    return interpRs
  }
  
    /*def interpolate(s1: List[Tuple2[java.sql.Timestamp, Double]], s2: List[Tuple2[java.sql.Timestamp, Double]]) :  List[Tuple2[Date, Double]] =  {
    val (ss1, ss2) = time2Double(s1, s2)
    
    val x = DenseVector(ss2.map(x=> x._1):_* )
    val y = DenseVector(ss2.map(y=> y._2):_* )
    
    val f = LinearInterpolator(x,y)
    val ss2yValues = ss1.map{ case (x,y) => f(x) }
    val interpRs = s1.map(x => x._1) zip ss2yValues
    
    return interpRs
  }*/
  
  
  def date2Double(s1: List[Tuple2[java.util.Date, Double]], s2: List[Tuple2[java.util.Date, Double]]) : Tuple2[ List[Tuple2[Double, Double]], List[Tuple2[Double,Double]]] = {

    
    val increment =  ((s2.length.toDouble-1) / (s1.length.toDouble-1)).toDouble
    
    val s2Array = s2.toArray
    val s1Array = s1.toArray
    var xAxis = 0.0
    var count = 0
    val s1Local = scala.collection.mutable.ListBuffer[Tuple2[Double,Double]]()
    
    while(count < s1Array.length) {
      s1Local.+=( (xAxis, s1Array(count)._2) )
      xAxis = xAxis + increment
      count = count + 1
    }
    
    xAxis = 0.0
    count = 0
    val s2Local = scala.collection.mutable.ListBuffer[Tuple2[Double,Double]]()
    while(count < s2Array.length) {
         s2Local.+=( (xAxis, s2Array(count)._2) )
      xAxis = xAxis + 1.0
      count = count + 1
     }
    (s1Local.toList, s2Local.toList )
  }
  
  
  def long2Double(s1: List[Tuple2[Long, Double]], s2: List[Tuple2[Long, Double]]) : Tuple2[ List[Tuple2[Double, Double]], List[Tuple2[Double,Double]]] = {

    
    val increment =  ((s2.length.toDouble-1) / (s1.length.toDouble-1)).toDouble
    
    val s2Array = s2.toArray
    val s1Array = s1.toArray
    var xAxis = 0.0
    var count = 0
    val s1Local = scala.collection.mutable.ListBuffer[Tuple2[Double,Double]]()
    
    while(count < s1Array.length) {
      s1Local.+=( (xAxis, s1Array(count)._2) )
      xAxis = xAxis + increment
      count = count + 1
    }
    
    xAxis = 0.0
    count = 0
    val s2Local = scala.collection.mutable.ListBuffer[Tuple2[Double,Double]]()
    while(count < s2Array.length) {
         s2Local.+=( (xAxis, s2Array(count)._2) )
      xAxis = xAxis + 1.0
      count = count + 1
     }
    (s1Local.toList, s2Local.toList )
  }
  
  
    def time2DoubleFmt(s1: List[Tuple2[java.util.Date, Double]], s2: List[Tuple2[java.util.Date, Double]]) : Tuple2[ List[Tuple2[Double, Double]], List[Tuple2[Double,Double]]] = {

    /*
     val rs =  s1.map { 
        x => (x._1
        .toString()
        .replaceAll(":", "")
        .replaceAll("-", "")
        .replaceAll(".", "")
        .replaceAll(" ", "") + ".0", x._2)
     }
     
      s1.map { x => 
      
   */
     val s1L = s1.map {
        x => ( (x._1.getTime + ".0").toDouble , x._2)
      }
      
     val s2L = s1.map {
        x => ( (x._1.getTime + ".0").toDouble , x._2)
      }
     
      
    (s1L, s2L )
  }
  

  
  
  def time2DenseVector(s: List[Tuple2[java.util.Date, Double]]) : Tuple2[DenseVector[Double],DenseVector[Double]] = {

    //12:00  12:01  12:04, 12:05
    val sArray = s.toArray
    val numIntervals = s.length
    val totalTime = s(numIntervals-1)._1.getTime - s(0)._1.getTime
    val avgDistance = totalTime/numIntervals
    var xAxis = 0.0
    var count = 0
    val localList = scala.collection.mutable.ListBuffer[Tuple2[Double,Double]]()
    while(count < numIntervals) {
      localList.+=( (xAxis, sArray(count)._2) )
      xAxis = xAxis + 1.0
      count = count + 1
    }
 
    val x = DenseVector(localList.map(x=> x._1):_* )
    val y = DenseVector(localList.map(x=> x._2):_* )
    
    return (x,y)
    
  }
  
}