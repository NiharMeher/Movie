from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("UDF Example") \
    .getOrCreate()

# Sample data
data = [("John", 25), ("Alice", 30), ("Bob", 35)]
df = spark.createDataFrame(data, ["name", "age"])

# Define a UDF
def greet(name):
    return "Hello, " + name

# Register the UDF
greet_udf = udf(greet, StringType())

# Use the UDF in DataFrame operations
df = df.withColumn("greeting", greet_udf(df["name"]))

# Show the result
df.show()
