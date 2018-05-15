// Start a simple Spark Session
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()

// Grab small dataset with some missing data
val df = spark.read.option("header","true").option("inferSchema","true").csv("ContainsNull.csv")

// Show schema
df.printSchema

// Notice the missing values!
df.show()

// You basically have 3 options with Null values
// 1. Just keep them, maybe only let a certain percentage through
// 2. Drop them
// 3. Fill them in with some other value
// No "correct" answer, you'll have to adjust to the data!

// Dropping values
// Technically still experimental, but it has been around since 1.3

// Drop any rows with any amount of na values
//df.na drop,fill,replace
df.na.drop().show()

// Drop any rows that have less than a minimum Number
// of NON-null values ( < Int)
df.na.drop(2).show() //Keep any rows that have at least one or more non-null values

df.na.fill(100).show()
df.na.fill("Missing Name").show()
df.na.fill("New name",Array("Name")).show()
df.na.fill(200,Array("Sales")).show()

df.describe().show() //Sales avg is 400.5
val df2 = df.na.fill(400.5,Array("Sales"))
df2.na.fill("missing name",Array("Name")).show()
