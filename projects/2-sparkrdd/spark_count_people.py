from pyspark import SparkContext, SparkConf
import sys

# Read command-line argument
_, source = sys.argv

# Initialize Spark Context
conf = SparkConf().setAppName("spark-count-people")
sc = SparkContext(conf=conf)

# Read file and extract cities
people_rdd = sc.textFile(source)
cities_rdd = people_rdd.map(lambda line: line.split()[2])  # Extract city from each line

# Count occurrences of each city
city_counts = cities_rdd.map(lambda city: (city, 1)).reduceByKey(lambda a, b: a + b)

# Collect and print results
result = city_counts.collect()

print(
    f"""
------------------------------------------
---------- People per City ---------------
------------------------------------------
"""
)
for city, count in result:
    print(f"{city}: {count}")

print(
    f"""
------------------------------------------
------------------------------------------
------------------------------------------
"""
)

# Stop Spark Context
sc.stop()
