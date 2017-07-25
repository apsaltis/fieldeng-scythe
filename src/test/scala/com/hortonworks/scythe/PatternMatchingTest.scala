package com.hortonworks.scythe

import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.scalatest.junit.AssertionsForJUnit

import java.text.SimpleDateFormat
import java.sql.Timestamp

class PatternMatchingTest {

  @Before def initialize() {
    println("Pattern Matching Tests")
  }
  
  @Test def findPatternWithin5PercentMatch() {
    val pm = new PatternMatching
    val rs = pm.findPattern(.05, p, s)
    
    for (m <- rs) {
      m.foreach(println)
      println("--")
    }
    
  
    val f = rs.flatMap(l => l.map(x => x._3))
   
    val sum = f.toList.sum  /// rs.map(x=>x.length)
    println(sum)
    
  }
  
  
  @Test def findPatternWithin85PercentMatch() {
    val pm = new PatternMatching
    val rs = pm.findPattern(.85, p, s)
    
    for (m <- rs) {
      m.foreach(println)
      println("--")
    }
  }
  
  
  private val df = new SimpleDateFormat("yyyy-MM-dd")
  private val d = new SimpleDateFormat("yyyy-MM-dd HH:mm")
  
  private val p = List(
    ("mpg1",new Timestamp (df.parse("2017-04-01 12:00").getTime), 0.0),
    ("mpg1",new Timestamp (df.parse("2017-04-02 12:01").getTime), 0.5),
    ("mpg1",new Timestamp (df.parse("2017-04-03 12:02").getTime), 1.0),
    ("mpg1",new Timestamp (df.parse("2017-04-04 12:03").getTime), 0.5),
    ("mpg1",new Timestamp (df.parse("2017-04-05 12:04").getTime), 1.0))
    
  private val s = List(
    ("mpg2",new Timestamp (d.parse("2017-04-01 12:00").getTime), 0.0),
    ("mpg2",new Timestamp (d.parse("2017-04-01 12:01").getTime), 0.5),
    ("mpg2",new Timestamp (d.parse("2017-04-01 12:02").getTime), 1.0),
    ("mpg2",new Timestamp (d.parse("2017-04-01 12:03").getTime), 0.5),
    ("mpg2",new Timestamp (d.parse("2017-04-01 12:04").getTime), 1.0),
    ("mpg2",new Timestamp (d.parse("2017-04-01 12:05").getTime), 0.0),
    ("mpg2",new Timestamp (d.parse("2017-04-01 12:06").getTime), 0.5),
    ("mpg2",new Timestamp (d.parse("2017-04-01 12:07").getTime), 1.0),
    ("mpg2",new Timestamp (d.parse("2017-04-01 12:08").getTime), 0.5),
    ("mpg2",new Timestamp (d.parse("2017-04-01 12:09").getTime), 1.0),
    ("mpg2",new Timestamp (d.parse("2017-04-01 12:10").getTime), 1.0),
    ("mpg2",new Timestamp (d.parse("2017-04-01 12:11").getTime), 1.0),
    ("mpg2",new Timestamp (d.parse("2017-04-01 12:12").getTime), 5.0))
}