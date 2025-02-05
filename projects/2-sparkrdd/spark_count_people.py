from pyspark import SparkContext, SparkConf
import sys

_, source = sys.argv

conf = SparkConf().setAppName("spark-count-people")
sc = SparkContext(conf=conf)

people_rdd = sc.textFile(source)

# Extract city from each line
cities_rdd = people_rdd.map(lambda line: line.split()[2])  

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

sc.stop()
