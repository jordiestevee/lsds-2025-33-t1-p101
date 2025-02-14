from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lower, regexp_replace, split
from pyspark.rdd import RDD
import sys

def preprocess_text(text: str) -> list:
    """Cleans and tokenizes text into words."""
    text = text.lower()
    text = regexp_replace(text, r'[^\w\s]', '')  # Remove punctuation
    words = split(text, '\\s+')  # Split into words
    return words

def generate_bigrams(words: list) -> list:
    """Generates bigrams from a list of words."""
    return [(words[i], words[i+1]) for i in range(len(words)-1)]

def main():
    if len(sys.argv) != 4:
        print("Usage: spark-submit spark_tweet_bigrams.py <language> <input_file> <output_dir>")
        sys.exit(1)
    
    language = sys.argv[1]
    input_file = sys.argv[2]
    output_dir = sys.argv[3]

    spark = SparkSession.builder.appName("TweetBigrams").getOrCreate()
    
    df = spark.read.json(input_file)
    tweets = df.filter(col("lang") == language).select("text")
    
    words_rdd: RDD = tweets.rdd.map(lambda row: preprocess_text(row.text))
    bigrams_rdd = words_rdd.flatMap(generate_bigrams)
    
    bigram_counts = bigrams_rdd.map(lambda bigram: (bigram, 1)).reduceByKey(lambda a, b: a + b)
    filtered_bigrams = bigram_counts.filter(lambda pair: pair[1] > 1)
    sorted_bigrams = filtered_bigrams.sortBy(lambda pair: pair[1], ascending=False)
    
    sorted_bigrams.saveAsTextFile(output_dir)
    
    spark.stop()

if __name__ == "__main__":
    main()