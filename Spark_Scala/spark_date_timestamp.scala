// Dates and TimeStamps

// Start a simple Spark Session
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()

// Create a DataFrame from Spark Session read csv
// Technically known as class Dataset
val df = spark.read.option("header","true").option("inferSchema","true").csv("CitiGroup2006_2008")

// Show Schema
df.printSchema()

// Lot's of options here
// http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$@add_months(startDate:org.apache.spark.sql.Column,numMonths:Int):org.apache.spark.sql.Column

df.head(2)

//Month
df.select(month(df("Date"))).show()

//year
df.select(year(df("Date"))).show()

val df2 = df.withColumn("Year",year(df("Date")))

val df2mins = df2.groupBy("Year").min()

df2mins.select($"Year",$"min(Close)").show()
