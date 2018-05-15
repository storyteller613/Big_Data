// GROUP BY and AGG (Aggregate methods)

// Start a simple Spark Session
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()

// Create a DataFrame from Spark Session read csv
// Technically known as class Dataset
val df = spark.read.option("header","true").option("inferSchema","true").csv("Sales.csv")

df.printSchema

// df.show()

// Groupby Categorical Columns

//mean
// df.groupBy("Company").mean().show()
//
// //count
// df.groupBy("Company").count().show()
//
// //max
// df.groupBy("Company").max().show()
//
// //min
// df.groupBy("Company").min().show()
//
// //sum
// df.groupBy("Company").sum().show()

// Other Aggregate Functions
// http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$
// df.select(sum("Sales")).show()
// df.select(countDistinct("Sales")).show() //approxCountDistinct
// df.select(sumDistinct("Sales")).show()
// df.select(variance("Sales")).show()
// df.select(stddev("Sales")).show() //avg,max,min,sum,stddev
// df.select(collect_set("Sales")).show()

df.show()

//orderBy, ascending
df.orderBy("Sales").show()

//orderBy, descending
df.orderBy($"Sales".desc).show()
