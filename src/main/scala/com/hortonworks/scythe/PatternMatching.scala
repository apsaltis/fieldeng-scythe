package com.hortonworks.scythe

import java.sql.Timestamp

class PatternMatching {

  /**
   * findPattern takes a series of tagName, timestamp, value and looks for matching patterns
   * in a series of data or a list of series.  In the case of list[list[]] it will be distributed
   * accuracy effects how similiar of a shape you want to find ex: .15 is 85% match
   */
  def findPattern(acc: Double, p: List[ Tuple3[String, java.sql.Timestamp, Double] ], s: List[Tuple3[String, java.sql.Timestamp, Double]] ) 
    : Array[Array[Tuple3[String, java.sql.Timestamp, Double]]] = {

    val pSlide = p.sliding(2, 1).toArray
    val pRatioArr = pSlide.map(x => (x(0)._1, x(0)._2, x(0)._3, x(0)._3 / x(1)._3))

    val slideR = s.sliding(2, 1).toArray
    val ratioRArr = slideR.map(x => (x(0)._1, x(0)._2, x(0)._3, x(0)._3 / x(1)._3))
    val sameLength = ratioRArr.sliding(p.length -1, 1).toArray

    import scala.collection.mutable

    val deltaArr = sameLength.map {
      sRatioArr =>
        var deltas = Array[Tuple5[String, java.sql.Timestamp, Double, Double, Double]]()
        var count = 0;
        while (count < sRatioArr.length) {
          deltas = deltas :+ (
            sRatioArr(count)._1,
            sRatioArr(count)._2,
            sRatioArr(count)._3,
            sRatioArr(count)._4,
            Math.abs(sRatioArr(count)._4 - pRatioArr(count)._4))
          count = count + 1
        }
        deltas
    }

    //deltaArr.foreach(println) 

    val scored = deltaArr.map {
      arr =>
        (arr, arr
          .map {
            case (tag, time, value, ratio, diff) => diff
          }.sum / arr.length)
    }

    val matches = scored.filter { case (ts, avgDiff) => (avgDiff < acc) }

    //matches.foreach(println)

   // case class TS(time: Int, value: Double)

    val tsArray = matches.map {
      tup =>
        tup._1.map {
          case (tag, time, value, ratio, diff) => (tag, time, value)
        }
    }

    return tsArray

  }

}