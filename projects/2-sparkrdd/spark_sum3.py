from pyspark import SparkContext, SparkConf
import sys

_, source = sys.argv

conf = SparkConf().setAppName("spark-sum-even-numbers")
sc = SparkContext(conf=conf)

numbers_rdd = sc.textFile(source)

# Split each line into individual numbers, flatten the list, and convert to integers
numbers_int_rdd = numbers_rdd.flatMap(lambda line: [int(num) for num in line.split()])

# Filter even numbers
even_numbers_rdd = numbers_int_rdd.filter(lambda x: x % 2 == 0)

# Compute the sum of even numbers
result = even_numbers_rdd.sum()

# Print the result
print(
    f"""
------------------------------------------
------------------------------------------
------------------------------------------
SUM OF EVEN NUMBERS = {result}
------------------------------------------
------------------------------------------
------------------------------------------
"""
)

sc.stop()
