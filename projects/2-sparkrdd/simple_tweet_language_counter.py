import json
from collections import defaultdict
from tweet_parser import parse_tweet, Tweet

def count_tweets_by_language(file_path: str):
    language_count = defaultdict(int)

    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line:
                continue  # Skip empty lines

            try:
                tweet = parse_tweet(line)
                language_count[tweet.language] += 1
            except (json.JSONDecodeError, KeyError, TypeError):
                continue  # Skip invalid JSON or missing fields

    return dict(language_count)

if __name__ == "__main__":
    file_path = "data/Eurovision3.json"
    language_counts = count_tweets_by_language(file_path)

    print("Tweet Count by Language:")
    print(language_counts)
