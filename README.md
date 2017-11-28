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


# Interpolation DataFrame

val map = ch.interpolate ("mpg1", List("mpg1", "mpg2"), "function", ds)

# Pattern Matching
95% precision

val rs = new PatternMatching.findPattern(.05, p, s)