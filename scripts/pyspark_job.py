from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("ETLJob").getOrCreate()

# Load data
df = spark.read.csv("data/employees.csv", header=True, inferSchema=True)

# Window spec
window_spec = Window.partitionBy("department")

# Transformation
df_with_avg = df.withColumn("avg_salary", avg("salary").over(window_spec))

result_df = df_with_avg.filter(col("salary") > col("avg_salary"))

# Save output
result_df.write.mode("overwrite").parquet("output/above_avg_salary")

spark.stop()
