from pyspark import SparkContext, SparkConf
import sys
import json

if len(sys.argv) != 4:
    print("Usage: spark_tweet_retweets.py <language_code> <input_file> <output_file>")
    sys.exit(-1)

language = sys.argv[1]
input_file = sys.argv[2]
output_file = sys.argv[3]

conf = SparkConf().setAppName("spark-tweet-retweets")
sc = SparkContext(conf=conf)

tweets = sc.textFile(input_file)


def safe_json_parse(line):
    try:
        return json.loads(line)
    except Exception:
        return None


tweets_parsed = tweets.map(safe_json_parse).filter(lambda tweet: tweet is not None)

tweets_filtered = tweets_parsed.filter(lambda tweet: tweet.get("lang") == language)

top_10_tweets = (
    tweets_filtered.sortBy(lambda tweet: tweet.get("retweet_count", 0), ascending=False)
    .zipWithIndex()
    .filter(lambda tup: tup[1] < 10)
    .map(lambda tup: tup[0])
)

top_10_tweets.saveAsTextFile(output_file)

sc.stop()
