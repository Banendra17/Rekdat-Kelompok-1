import tweepy
import pandas as pd
import json
from datetime import datetime


local_filepath = f"../data/tweets.csv"


def execute_twitter_etl():

    # Twitter authentication
    api_key = "CPyHoCo8IFvLggqxAdDExhHlI"
    api_secret_key = "XadKFgrmsE74Ijd2NPjTso9Eg88QotMjQBwgYH9NhNwBiHBEWS"
    access_token = "1589554347272474626-4iZCP5f1goD69T9d6MAubIrFNz5ae0"
    access_token_secret = "wgNXWBHwLVkOXypH0rBEfBqSDnykzZSCwZli1SRT2byx3"

    auth = tweepy.OAuthHandler(api_key, api_secret_key)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth, wait_on_rate_limit=True)

    hashtag = '#Spotify'
    query = tweepy.Cursor(api.search_tweets, q=hashtag,
                          tweet_mode='extended').items(100)
    # print(tweets)

    list = []
    for tweet in query:
        if 'retweeted_status' in tweet._json:
            text = tweet._json['retweeted_status']["full_text"]
        else:
            text = tweet.full_text

        refined_tweet = {"user": tweet.user.screen_name,
                         'text': text,
                         'created_at': tweet.created_at}

        list.append(refined_tweet)

    df = pd.DataFrame(list)
    # df.to_csv(r'C:\Users\daffa\docker\airflow\refined_tweets.csv')
