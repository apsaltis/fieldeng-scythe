package com.hortonworks.scythe

import java.text.SimpleDateFormat
import com.hortonworks.scythe.functions._
import java.util.Calendar;
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.sql.{ Date, Timestamp }

class Sample(var fmt: String = "yyyy-MM-dd HH:mm") {
  /**
   * "D" => Day, "H" => Hour, "M" => Minute, "S" => Second
   * agg => AVG, SUM, MIN, MAX, LAST
   */
  def downSample(rate: String, agg: String, s: List[Tuple2[java.util.Date, Double]]) : List[Tuple2[String, Double]] = {
    
    //var fmt = "yyyy-MM-dd" // default rate
    rate match {
      case "Y" => fmt = "yyyy"
      case "Month" => fmt = "yyyy-MM"
      case "D" => fmt = "yyyy-MM-dd"
      case "H" => fmt = "yyyy-MM-dd HH"
      case "M" => fmt = "yyyy-MM-dd HH:mm"
      case "S" => fmt = "yyyy-MM-dd HH:mm:ss"
    }
    
    val rtn = s.groupBy(f => 
      new SimpleDateFormat(fmt).format(f._1)) 
    .mapValues(values => values.map(_._2).toIterable.avg)
   
   rtn.toList 
  }

  def downSample(rate: String, agg: String, ds: DataFrame, tsCol:String = "time") : DataFrame = {
  /**
   * "D" => Day, "H" => Hour, "M" => Minute, "S" => Second
   * agg => AVG, SUM, MIN, MAX, LAST
   */
    
    //var fmt = "yyyy-MM-dd HH:mm"  // minute default
    rate match {
      case "Y" => fmt = "yyyy"  
      case "Month" => fmt ="yyyy-MM"
      case "D" => fmt = "yyyy-MM-dd"
      case "H" => fmt = "yyyy-MM-dd HH"
      case "M" => fmt = "yyyy-MM-dd HH:mm"
      case "S" => fmt = "yyyy-MM-dd HH:mm:ss"
    }
    
    val c = date_format(ds(tsCol), fmt)  
    val ds1 = ds.withColumn("time_bin", c )
    
    ds1.groupBy("time_bin").avg("value").orderBy("time_bin")
  }
}
