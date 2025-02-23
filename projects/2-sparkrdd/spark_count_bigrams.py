from pyspark import SparkContext, SparkConf
import sys

_, source = sys.argv

conf = SparkConf().setAppName("spark-count-bigrams")
sc = SparkContext(conf=conf)

# Read the file into an RDD
lines_rdd = sc.textFile(source)

# Split each line into words and generate bigrams
bigrams_rdd = lines_rdd.flatMap(
    lambda line: [
        ((line.split()[i], line.split()[i + 1]), 1)
        for i in range(len(line.split()) - 1)
    ]
)

# Count the occurrences of each bigram
bigram_counts_rdd = bigrams_rdd.reduceByKey(lambda a, b: a + b)

# Collect the results and print
results = bigram_counts_rdd.collect()
for bigram, count in results:
    print(f"Bigram: {bigram}, Count: {count}")

sc.stop()
