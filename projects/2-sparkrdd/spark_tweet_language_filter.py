from pyspark import SparkContext, SparkConf
import sys
import json

if len(sys.argv) != 4:
    print("Usage: spark_tweet_language_filter.py <language_code> <input_file> <output_file>")
    sys.exit(-1)

language = sys.argv[1]
input_file = sys.argv[2]
output_file = sys.argv[3]

conf = SparkConf().setAppName("spark-tweet-language-filter")
sc = SparkContext(conf=conf)

tweets = sc.textFile(input_file)

def filter_by_language(line):
    try:
        tweet = json.loads(line)
        # Check if the tweet has the required language code
        return tweet.get("lang") == language
    except Exception as e:
        # If there's an error parsing the JSON, ignore this line
        return False

filtered_tweets = tweets.filter(filter_by_language)

filtered_tweets.saveAsTextFile(output_file)

sc.stop()
