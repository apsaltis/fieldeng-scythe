package com.hortonworks.scythe

package object functions {
  
  def average[T]( elems: Iterable[T] )( implicit num: Numeric[T] ) = {
    num.toDouble( elems.sum ) / elems.size
  }
  
  implicit def iterebleWithAvg[T:Numeric](data:Iterable[T]) = new {
    def avg = average(data)
  }
  
  
  def movingAverage [T](elems: Iterable[T])(size: Int)( implicit num: Numeric[T] ) = {
    elems.sliding(size, 1).map { _.avg }
  }
  
  implicit def iterebleWithMovAvg3[T:Numeric](data:Iterable[T]) = new {
    def movAvg3 = movingAverage(data)(3)
  }
  
  /*def movingAverage5 [T](elems: Iterable[T])( implicit num: Numeric[T] ) = {
    elems.sliding(5, 1).map { _.avg }
  }*/
  
  implicit def iterebleWithMovAvg5[T:Numeric](data:Iterable[T]) = new {
    def movAvg5 = movingAverage(data)(5)
  }
  
  
   implicit def iterebleWithTWMA[T:Numeric](data:Iterable[T]) = new {
    def twma = movingAverage(data)(5)
  }
  
  
  /* 
   * spark
   */
 
  /*
   * mov5 Int
   */
  def sparkMovingAverage5Int (elems: org.apache.spark.rdd.RDD[Int])( implicit num: Numeric[Int] ) = {
     val e = org.apache.spark.mllib.rdd.RDDFunctions.fromRDD(elems)
     e.sliding(5, 1).map { x => x.toIterable.avg }
  }
  
  implicit def iterebleWithMovAvg5Int(data: org.apache.spark.rdd.RDD[Int]) = new {
    def movAvg5 = sparkMovingAverage5Int(data)
  }
  
  /*
   * mov5 Double
   */
   def sparkMovingAverage5Double (elems: org.apache.spark.rdd.RDD[Double])( implicit num: Numeric[Int] ) = {
     val e = org.apache.spark.mllib.rdd.RDDFunctions.fromRDD(elems)
     e.sliding(5, 1).map { x => x.toIterable.avg }
  }
  
  
  implicit def iterebleWithMovAvg5Double(data: org.apache.spark.rdd.RDD[Double]) = new {
    def movAvg5 = sparkMovingAverage5Double(data)
  }
  
    /*
   * spark
   * mov3 Int
   */
  def sparkMovingAverage3Int (elems: org.apache.spark.rdd.RDD[Int])( implicit num: Numeric[Int] ) = {
     val e = org.apache.spark.mllib.rdd.RDDFunctions.fromRDD(elems)
     e.sliding(3, 1).map { x => x.toIterable.avg }
  }
  
  implicit def iterebleWithMovAvg3Int(data: org.apache.spark.rdd.RDD[Int]) = new {
    def movAvg3 = sparkMovingAverage3Int(data)
  }
  /*
   * spark
   * mov3 Double
   */
   def sparkMovingAverage3Double (elems: org.apache.spark.rdd.RDD[Double])( implicit num: Numeric[Int] ) = {
     val e = org.apache.spark.mllib.rdd.RDDFunctions.fromRDD(elems)
     e.sliding(3, 1).map { x => x.toIterable.avg }
  }
  
  
  implicit def iterebleWithMovAvg3Double(data: org.apache.spark.rdd.RDD[Double]) = new {
    def movAvg3 = sparkMovingAverage3Double(data)
  }
  
  
  /*
   org.apache.spark.mllib.rdd.RDDFunctions.fromRDD(
  
  
       val movAvg3 = tempList.sliding(3,1).map{ l => l.map(x => x._2).sum / l.size}.toArray

// time weighted moving avg, step wise
val twa =  tempList.sliding(3,1).map {
    arr => 
    var tws = 0.0
    var td = 0.0 // time delta step intervals
    for (i <- 0 until arr.length) {
        if (i == ( arr.length -1 )) {
            td  += 1
            tws += ( 1 * arr(i)._2 )
        }
        else {
            td  += ( arr(i+1)._1 - arr(i)._1 )
            tws += ( ( arr(i+1)._1 - arr(i)._1 ) * arr(i)._2 ) 
        }
    }
    tws / td
}.collect
       */
}