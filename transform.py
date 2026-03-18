from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Demo").getOrCreate()

df = spark.read.csv("/Users/as-mac-1202/Pyspark_Databricks_Practice/Pyspark_Databricks_Practice/sales copy.csv", header=True, inferSchema=True)

df.show()

# Transformation 1 (example)
df = df.withColumn("new_revenue", col("revenue") * 2)

df.show()