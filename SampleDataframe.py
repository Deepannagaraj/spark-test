from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, sum
from datetime import datetime
import time

def get_current_time():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

spark = SparkSession.builder.appName("SparkExample") \
    .enableHiveSupport() \
    .getOrCreate()

"""
    .config("spark.files", "/home/cdsw/log4j2.properties") \
    .config("spark.driver.extraJavaOptions", "-Dlog4j.configurationFile=log4j2.properties") \
    .config("spark.executor.extraJavaOptions", "-Dlog4j.configurationFile=log4j2.properties") \
"""

spark.sparkContext.setLogLevel("WARN")

def wait_three_minutes(x):
    # 180 seconds = 3 minutes
    time.sleep(1)
    return x

sleep_udf = udf(wait_three_minutes, IntegerType())

print(f"Start Time: {get_current_time()}")

df = spark.range(0, 1)
df_with_delay = df.withColumn("delayed_col", sleep_udf("id"))
df_with_delay.show() 

print(f"End Time:   {get_current_time()}")

data = [
("2024-02-01", "Electronics", 100),
("2024-02-01", "HomeGoods", 200),
("2024-02-02", "Electronics", 150),
("2024-02-02", "HomeGoods", 250),
("2024-02-03", "Electronics", 300),
]
columns = ["date", "department", "sales"]

df = spark.createDataFrame(data, columns)
df_filtered = df.filter(col("sales") > 150)

df_grouped = df_filtered.groupBy("department").agg(sum("sales").alias("total_sales"))
df_grouped.show()

grouped_counts = df.groupBy("department").count()

grouped_counts.show()
print("Grouped counts:")
grouped_counts.count()

grouped_counts_renamed = grouped_counts.withColumnRenamed("count", "num_employees")
grouped_counts_renamed.show()
