# fieldeng-scythe
Time Series Library


| Time      | TagName     | Value |
| ----------|:-----------:| -----:|
| 12:00     | signal1 	  | 0     |
| 12:01     | signal1     | 0.5   |
| 12:02 	| signal1     | 1     |
| 12:03		| signal1	  | 0.5   |
| 12:04		| signal1	  | 1     |	


# Interpolation

val rs = new LinearInterpolation.interpolateDate(sig1Raw, sig2Raw)


![alt tag](https://github.com/hortonworks/fieldeng-scythe/blob/master/linear-tables.png)

# Start Spark shell with dependencies
```
spark-shell --jars target/scythe-0.0.1-SNAPSHOT.jar
```

# Load Sample data from CSV
```
import spark.implicits._
import spark.sqlContext._

val df = spark.read.format("com.databricks.spark.csv")
  .option("header", "true")
  .option("delimiter", ",")
  .option("inferSchema", "true")
  .load("src/test/resources/example.csv")

df.printSchema
df.show
```

# Interpolate one tag against the other
```
import com.hortonworks.scythe.cronus.Helper
val ch = Helper
val map = ch.interpolate ("mpg1", List("mpg1", "mpg2"), "function", ds)
```

# Interpolate values for tag "ss" to align to "ps"
```
val map = ch.interpolate (
  "ps", List("ps", "ss"), 
  "tagName", "yyyy-MM-dd HH:mm", df
)
    
map.values.foreach { println }
```

# Downsample a signal
```
val hourlyResampled = new Sample().downSample("H", "AVG", df).select("avg(value)").collect
```

# Pattern Matching
95% precision

val rs = new PatternMatching.findPattern(.05, p, s)
