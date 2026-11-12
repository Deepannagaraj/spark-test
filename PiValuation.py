import random
from pyspark.sql import SparkSession

def main():
    # Initialize a SparkSession
    spark = SparkSession.builder \
        .appName("PySpark Pi Estimation") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("DEBUG")

    # Define the number of samples (higher means more accuracy)
    num_samples = 9999999
    
    # Define a helper function to check if a random point falls inside the unit circle
    def inside(p):
        x, y = random.random(), random.random()
        return x*x + y*y < 1

    # Parallelize the operation across the cluster
    # spark.sparkContext.parallelize creates an RDD from the sample range
    count = spark.sparkContext.parallelize(range(0, num_samples)) \
        .filter(inside) \
        .count()

    # Calculate the approximation of Pi
    # Area of circle / Area of square = (Pi * r^2) / (4 * r^2) = Pi / 4
    # Therefore, Pi = 4 * (points inside circle) / (total points)
    pi_estimate = 4.0 * count / num_samples

    print("\n" + "="*40)
    print(f"Pi is roughly: {pi_estimate}")
    print("="*40 + "\n")

    # Stop the Spark session to release resources
    spark.stop()

if __name__ == "__main__":
    main()
