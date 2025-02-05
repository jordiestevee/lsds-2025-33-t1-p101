from pyspark import SparkContext, SparkConf
import sys

# Read command-line argument
_, source = sys.argv

# Initialize Spark Context
conf = SparkConf().setAppName("spark-sum2")
sc = SparkContext(conf=conf)

# Read file and process numbers
numbers_rdd = sc.textFile(source)
numbers_int_rdd = numbers_rdd.flatMap(lambda line: map(int, line.split()))
result = numbers_int_rdd.sum()

# Print result
print(
    f"""
------------------------------------------
------------------------------------------
------------------------------------------
SUM = {result}
------------------------------------------
------------------------------------------
------------------------------------------
"""
)

# Stop Spark Context
sc.stop()
