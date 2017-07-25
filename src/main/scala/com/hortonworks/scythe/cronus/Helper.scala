package com.hortonworks.scythe.cronus

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.sql.{ Date, Timestamp }
import com.hortonworks.scythe.LinearInterpolation
import java.text.SimpleDateFormat

class Helper {

  def show[T](ds: org.apache.spark.sql.Dataset[T]): Unit = {
    ds.show()
  }

  def interpolate[T](s1: String, s2: List[String], df: org.apache.spark.sql.DataFrame) {
    interpolate(s1, s2, "function", df)
  }

  /*def interpolate[T](s1: String, s2: List[String], ds: org.apache.spark.sql.Dataset[T] )  {
    interpolate(s1, s2, "function", ds.toDF())      
  }*/

  def interpolate(s1: String, s2: List[String], colSigName: String, df: org.apache.spark.sql.DataFrame): Map[String, List[(String, java.util.Date, Double)]] = {

    var map = Map[String, List[(String, java.util.Date, Double)]]()

    val ds1 = df.filter(df.col(colSigName) === s1)
    val s = ds1.collect()

    // tagName, timestamp, value
    val rs = s.map { x =>
      ( x(0).toString(),
        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").parse(x(1).toString()),
        x(2).asInstanceOf[Double])
    }.toList

    map = map + (s1 -> rs)

    for (sigName <- s2) {

      if (sigName != s1) { // do not interpolate to the base signal
        val ds2 = df.filter(s"${colSigName} == '${sigName}'")

        val rs2 = ds2.collect.map { x =>
          (
            x(0).toString(),
            new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").parse(x(1).toString()),
            x(2).asInstanceOf[Double])
        }.toList

        val l = new LinearInterpolation
        val interp = l.interpolateDate(
          rs.map { case (tag, time, value) => (time, value) },
          rs2.map { case (tag, time, value) => (time, value) })

        map = map + (sigName -> (rs2.map(x => x._1) zip interp).map(x => (x._1, x._2._1, x._2._2)))
      }
    }
    return map
  }

}