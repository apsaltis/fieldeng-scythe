package com.hortonworks.scythe

import java.text.SimpleDateFormat
import com.hortonworks.scythe.functions._
import java.util.Calendar;
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.sql.{ Date, Timestamp }

class Sample() {

  def downSample(rate: String, agg: String, s: List[Tuple2[java.util.Date, Double]]) : List[Tuple2[String, Double]] = {
    val rtn = s.groupBy(f => 
      new SimpleDateFormat(formatStr(rate)).format(f._1)) 
    .mapValues(values => values.map(_._2).toIterable.avg)
   
   rtn.toList 
  }

  def formatStr(rate: String) : String = {
  /**
   * "D" => Day, "H" => Hour, "M" => Minute, "S" => Second
   * agg => AVG, SUM, MIN, MAX, LAST
   */

    rate match {
      case "Y" =>  "yyyy"
      case "Month" =>  "yyyy-MM"
      case "D" => "yyyy-MM-dd"
      case "H" => "yyyy-MM-dd HH"
      case "M" => "yyyy-MM-dd HH:mm"
      case "S" => "yyyy-MM-dd HH:mm:ss"
    }
  }

  def downSample(rate: String, agg: String, ds: DataFrame, tsCol:String = "time", valCol:String = "value") : DataFrame = {
    val c = date_format(ds(tsCol), formatStr(rate))  

    //Breaks unit test
    val format = List(IntegerType, DoubleType).contains(ds.schema(tsCol).dataType)
    val ds1 = format match {
      case true => ds.withColumnRenamed(tsCol, "time_bin")
      case false => ds.withColumn("time_bin", c)
    }

    //TODO: support multiple aggregation types
    ds1.groupBy("time_bin").avg(valCol).orderBy("time_bin")
  }
}
