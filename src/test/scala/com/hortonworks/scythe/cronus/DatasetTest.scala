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
  
  case class Signal(tagNm: String, date: java.sql.Timestamp, value: Double )
  
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
  //import sqlContext.implicits._

    val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
                                  
  
  val ds = Seq(                                 
      ("mpg1", "2017-04-01T11:05:27.000Z", 22.0),
      ("mpg1", "2017-04-01T11:05:28.000Z", 32.0),
      ("mpg1", "2017-04-01T11:05:29.000Z", 42.0),
      
      ("mpg2", "2017-04-01T11:05:27.000Z", 23.0),
      ("mpg2", "2017-04-01T11:05:29.000Z", 43.0)
      ).toDF("function", "time", "value")

     ds.show() 
     print(ds)
      
    val rs = ch.interpolate("mpg1", List("mpg2"), "function", ds)

    println(rs)

   /* for (i <- 0 until rs.size) {
      Assert.assertEquals(expectedResult(i)._1, rs(i)._1)
      Assert.assertEquals(expectedResult(i)._2, rs(i)._2, 0.001)
    }*/
  }
  
  //private
  //case class Signal(tagNm: String, date: java.sql.Timestamp, value: Double )
  
 
  
}