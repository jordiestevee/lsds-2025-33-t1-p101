from pyspark import SparkContext, SparkConf
import sys

_, source = sys.argv

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

sc.stop()
