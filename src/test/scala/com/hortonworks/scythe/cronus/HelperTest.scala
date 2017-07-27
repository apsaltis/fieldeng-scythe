package com.hortonworks.scythe.cronus

import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.scalatest.junit.AssertionsForJUnit

import org.apache.spark.sql._
import java.text.SimpleDateFormat
import org.apache.spark
//import sqlContext.implicits._
import org.apache.spark.sql.SparkSession

class HelperTest {

  case class Signal(tagNm: String, date: java.sql.Timestamp, value: Double)

  @Before def initialize() {
    println("Dataset Tests")
  }

  @Test def interpolate() {
    val ch = new com.hortonworks.scythe.cronus.Helper

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Scythe Test Cases")
      .getOrCreate()

    import spark.implicits._
    import spark.sqlContext._

    val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

    val ds = Seq(
      ("mpg1", "2017-04-01T11:05:27.000Z", 22.0),
      ("mpg1", "2017-04-01T11:05:28.000Z", 32.0),
      ("mpg1", "2017-04-01T11:05:29.000Z", 42.0),

      ("mpg2", "2017-04-01T11:05:27.000Z", 23.0),
      ("mpg2", "2017-04-01T11:05:29.000Z", 43.0)).toDF("function", "time", "value")

    ds.show()
    print(ds)

    val map = ch.interpolate("mpg1", List("mpg1", "mpg2"), "function", ds)

    println(map)

    val mpg1 = map.get("mpg1")
    val mpg2 = map.get("mpg2")
    Assert.assertEquals(22.0, mpg1.get(0)._3, 0.001)
    Assert.assertEquals(33.0, mpg2.get(1)._3, 0.001)
  }

}