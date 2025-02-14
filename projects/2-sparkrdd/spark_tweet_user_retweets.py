from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

import sys

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark-submit spark_tweet_user_retweets.py <language> <input_path>")
        sys.exit(1)

    language = sys.argv[1]  
    input_path = sys.argv[2]  

    spark = SparkSession.builder.appName("TopRetweetedUsers").getOrCreate()

    df = spark.read.json(input_path)

    # Filter tweets by language and select relevant columns
    filtered_df = df.filter(col("lang") == language).select("user.screen_name", "retweet_count")

    # Aggregate retweets per user
    retweet_counts = (
        filtered_df.groupBy("screen_name")
        .agg(spark_sum("retweet_count").alias("total_retweets"))
    )

    # Get top 10 users by retweets
    top_users = retweet_counts.orderBy(col("total_retweets").desc()).limit(10)

    top_users.show(truncate=False)

    spark.stop()
