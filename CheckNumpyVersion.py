import inspect
import logging
import socket
import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("NodeManager-Python-Details").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    num_parts = 60

    import socket

    def check_executor_numpy(partition):
        try:
            import numpy
            version = numpy.__version__
            path = numpy.__file__
        except Exception as e:
            version = f"Error: {str(e)}"
            path = "N/A"
        
        hostname = socket.gethostname()
        yield (hostname, version, path)

    # Create a small parallelized RDD across your executors
    rdd = spark.sparkContext.parallelize(range(num_parts), numSlices=num_parts)
    results = rdd.mapPartitions(check_executor_numpy).distinct().collect()

    print(f"{'Hostname':<27} | {'NumPy Version':<15} | {'Path'}")
    print("-" * 100)
    for host, ver, path in results:
        print(f"{host:<25} | {ver:<15} | {path}")

    spark.stop()
