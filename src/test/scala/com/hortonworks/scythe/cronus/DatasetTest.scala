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

class DatasetTest {
  
  case class Signal(tagNm: String, date: java.sql.Timestamp, value: Double )
  
  @Before def initialize() {
    println("Dataset Tests")
  }
  
  @Test def interpolate() {
    val d = new com.hortonworks.scythe.cronus.Dataset
  
    
   val spark = SparkSession
    .builder()
     .master("local")
    .appName("Scythe Test Cases")
    .getOrCreate()

  import spark.implicits._
  import spark.sqlContext._
  //import sqlContext.implicits._

    val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    //case class Signal(tagNm: String, date: java.sql.Timestamp, value: Double )
   
  val ds = Seq(
      Signal("mpg1", new java.sql.Timestamp ( df.parse("2017-04-01T11:05:27.444Z").getTime), 22.0),
      Signal("mpg1", new java.sql.Timestamp (df.parse("2017-04-01T11:05:28.444Z").getTime), 32.0),
      Signal("mpg1", new java.sql.Timestamp (df.parse("2017-04-01T11:05:29.444Z").getTime), 42.0),
      
      Signal("mpg2", new java.sql.Timestamp (df.parse("2017-04-01T11:05:27.444Z").getTime), 23.0),
      Signal("mpg2", new java.sql.Timestamp (df.parse("2017-04-01T11:05:29.444Z").getTime), 43.0)
      ).toDF()

    //val gg = spark.sparkContext.parallelize(ds)
    //gg.toDF()
      
     ds.show() 
     print(ds)
      
    val rs = d.interpolate("mpg1", List("mpg2"), "function", ds)

    println(rs)

   /* for (i <- 0 until rs.size) {
      Assert.assertEquals(expectedResult(i)._1, rs(i)._1)
      Assert.assertEquals(expectedResult(i)._2, rs(i)._2, 0.001)
    }*/
  }
  
  //private
  //case class Signal(tagNm: String, date: java.sql.Timestamp, value: Double )
  
 
  
}