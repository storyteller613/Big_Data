import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()

// Create a DataFrame from Spark Session read csv
// Technically known as class Dataset
val df = spark.read.option("header","true").option("inferSchema","true").csv("CitiGroup2006_2008")

// Show Schema
df.printSchema()

import spark.implicits._

// Grabbing all rows where a column meets a condition
df.filter($"Close">480).show()
df.filter("Close>480").show()

df.filter($"Close"<480 && $"High"<480).show()
df.filter("Close<480 AND High<480").show()

val CH_low = df.filter("Close<480 AND High<480").collect()

val CH_low_c = df.filter("Close<480 AND High<480").count()

df.filter($"High"===484.40).show()
df.filter("High = 484.40").show()

//correlation
df.select(corr("High","Low")).show()

// Operations and Useful Functions
// http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$
