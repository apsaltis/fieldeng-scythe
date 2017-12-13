package com.hortonworks.scythe

import java.text.SimpleDateFormat

import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.junit.Ignore

import breeze.interpolation.LinearInterpolator
import breeze.linalg.DenseVector
import com.hortonworks.scythe.functions._

import org.apache.spark
//import sqlContext.implicits._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class ResampleTest {
  
  @Before def initialize() {
    println("Sample Tests")
  }
  
  @Test def down() {
    
    val path = getClass.getResource("/downsample.csv").getPath
    
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Scythe Test Cases")
      .getOrCreate()

    import spark.implicits._
    import spark.sqlContext._

    val ds = spark.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .load(path)

    val valH = new Resample().down("H", "AVG", ds).select("avg(value)").collect
    Assert.assertEquals("Hour", 1.0, valH(0).getDouble(0), 0.0)
    
    val valM = new Resample().down("M", "AVG", ds).select("avg(value)").collect
    Assert.assertEquals("Minute", 0.25, valM(0).getDouble(0), 0.0)

    val valD = new Resample().down("D", "AVG", ds).select("avg(value)").collect
    Assert.assertEquals("Day", 1.0, valD(0).getDouble(0), 0.0)
  }
  
  @Ignore def downScratch() {
    
    val path = getClass.getResource("/downsample.csv").getPath
    
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Scythe Test Cases")
      .getOrCreate()

    import spark.implicits._
    import spark.sqlContext._

    val ds = spark.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .load(path)
    
    ds.printSchema
    ds.show
     
    val c = date_format($"time", "yyyy-MM-dd HH:mm")
      
    val ds1 = ds.withColumn("time_bin", c )
    ds1.show
    
    val ds2 = ds1.groupBy("time_bin").avg("value").orderBy("time_bin")
    ds2.show    
    
    //ds.createTempView("dataset")
    //val dd = spark.sql("SELECT avg(value) as value, FROM_UNIXTIME(time,'YYYY-MM-dd HH:mm') as time_bin  FROM dataset group by FROM_UNIXTIME(time,'YYYY-MM-dd HH:mm')")
    //dd.show
  }
}
