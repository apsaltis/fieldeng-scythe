package com.hortonworks.scythe

import java.text.SimpleDateFormat

import org.junit.Assert
import org.junit.Before
import org.junit.Test

import breeze.interpolation.LinearInterpolator
import breeze.linalg.DenseVector

class LinearInterpolationTest {

  @Before def initialize() {
    println("Interpolation Tests")
  }
  
  @Test def interpolateDate() {
    val l = new LinearInterpolation
    val rs = l.interpolateDate(sig1Raw, sig2Raw)

    println(rs)

    for (i <- 0 until rs.size) {
      Assert.assertEquals(expectedResult(i)._1, rs(i)._1)
      Assert.assertEquals(expectedResult(i)._2, rs(i)._2, 0.001)
    }
  }

  
  @Test def interpolateLong() {
    val l = new LinearInterpolation
    val rs = l.interpolateLong(
        sig1Raw.map{case (t,d) => (t.getTime, d)}, 
        sig2Raw.map{case (t,d) => (t.getTime, d)}
    )

    println(rs)

    for (i <- 0 until rs.size) {
      Assert.assertEquals(expectedResult(i)._1.getTime, rs(i)._1)
      Assert.assertEquals(expectedResult(i)._2, rs(i)._2, 0.001)
    }
  }
  
  @Test def linearInterpolate() {

    val x = DenseVector(s2.map(x => x._1): _*)
    val y = DenseVector(s2.map(x => x._2): _*)

    val f = LinearInterpolator(x, y)

    val rs = s1.map { case (x, y) => f(x) }

    val actual = rs.toArray
    val expected = Array(2.0, 2.75, 3.5, 4.25, 5.0, 4.0, 3.0, 2.0, 1.0, 1.25, 1.5, 1.75, 2.0)
    for (i <- 0 until actual.length) {
      Assert.assertEquals(expected(i), actual(i), 0.001)
    }
  }

  @Test def date2Double() {

    val l = new LinearInterpolation
    val (s1, s2) = l.date2Double(sig1Raw, sig2Raw)

    println(s1)
    println(s2)

    val x = DenseVector(s2.map(x => x._1): _*)
    val y = DenseVector(s2.map(x => x._2): _*)

    val f = LinearInterpolator(x, y)

    val ys = s1.map { case (x, y) => f(x) }

    println(ys)

    val actual = ys.toArray
    val expected = Array(2.0, 2.75, 3.5, 4.25, 5.0, 4.0, 3.0, 2.0, 1.0, 1.25, 1.5, 1.75, 2.0)
    for (i <- 0 until actual.length) {
      Assert.assertEquals(expected(i), actual(i), 0.001)

    }

  }

  
  /**
   * Common lists for test use-cases 
   */
  
  private val df = new SimpleDateFormat("yyyy-MM-dd")
  private val d = new SimpleDateFormat("yyyy-MM-dd HH:mm")

  private val signal1 = List(
    (df.parse("2017-04-01"), 1.0),
    (df.parse("2017-04-02"), 2.0),
    (df.parse("2017-04-03"), 3.0),
    (df.parse("2017-04-04"), 4.0),
    (df.parse("2017-04-05"), 5.0))

  private val signal2 = List(
    (df.parse("2017-04-01"), 1.0),
    (df.parse("2017-04-05"), 5.0))

  private val s1 = List((0.0, 0.0), (0.25, 0.5), (0.50, 1.0), (0.75, 0.5), (1.0, 1.0), (1.25, 0.0), (1.5, 0.5),
    (1.75, 1.0), (2.0, 0.5), (2.25, 1.0), (2.5, 1.0), (2.75, 1.0), (3.0, 5.0))

  private val s2 = List((0.0, 2.0), (1.0, 5.0), (2.0, 1.0), (3.0, 2.0))

  private val sig1Raw = List(
    (d.parse("2017-04-01 12:00"), 0.0),
    (d.parse("2017-04-01 12:01"), 0.5),
    (d.parse("2017-04-01 12:02"), 1.0),
    (d.parse("2017-04-01 12:03"), 0.5),
    (d.parse("2017-04-01 12:04"), 1.0),
    (d.parse("2017-04-01 12:05"), 0.0),
    (d.parse("2017-04-01 12:06"), 0.5),
    (d.parse("2017-04-01 12:07"), 1.0),
    (d.parse("2017-04-01 12:08"), 0.5),
    (d.parse("2017-04-01 12:09"), 1.0),
    (d.parse("2017-04-01 12:10"), 1.0),
    (d.parse("2017-04-01 12:11"), 1.0),
    (d.parse("2017-04-01 12:12"), 5.0))

  private val sig2Raw = List(
    (d.parse("2017-04-01 12:00"), 2.0),
    (d.parse("2017-04-01 12:04"), 5.0),
    (d.parse("2017-04-01 12:08"), 1.0),
    (d.parse("2017-04-01 12:12"), 2.0))

  private val expectedResult = List(
    (d.parse("2017-04-01 12:00"), 2.0),
    (d.parse("2017-04-01 12:01"), 2.75),
    (d.parse("2017-04-01 12:02"), 3.5),
    (d.parse("2017-04-01 12:03"), 4.25),
    (d.parse("2017-04-01 12:04"), 5.0),
    (d.parse("2017-04-01 12:05"), 4.0),
    (d.parse("2017-04-01 12:06"), 3.0),
    (d.parse("2017-04-01 12:07"), 2.0),
    (d.parse("2017-04-01 12:08"), 1.0),
    (d.parse("2017-04-01 12:09"), 1.25),
    (d.parse("2017-04-01 12:10"), 1.5),
    (d.parse("2017-04-01 12:11"), 1.75),
    (d.parse("2017-04-01 12:12"), 2.0))

}