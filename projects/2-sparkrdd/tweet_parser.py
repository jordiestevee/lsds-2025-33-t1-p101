from dataclasses import dataclass
from typing import Optional
import json


@dataclass
class Tweet:
    tweet_id: int
    text: str
    user_id: int
    user_name: str
    language: str
    timestamp_ms: int
    retweeted_id: Optional[int] = None
    retweeted_user_id: Optional[int] = None


def parse_tweet(tweet: str) -> Tweet:
    tweet_data = json.loads(tweet)
    return Tweet(
        tweet_id=tweet_data["id"],
        text=tweet_data["text"],
        user_id=tweet_data["user"]["id"],
        user_name=tweet_data["user"]["name"],
        language=tweet_data["lang"],
        timestamp_ms=tweet_data["timestamp_ms"],
        retweeted_id=tweet_data.get("retweeted_status", {}).get("id"),
        retweeted_user_id=tweet_data.get("retweeted_status", {})
        .get("user", {})
        .get("id"),
    )


if __name__ == "__main__":
    with open("data/Eurovision3.json", "r") as file:
        first_line = file.readline()
        parsed_tweet = parse_tweet(first_line)
        print(parsed_tweet)
