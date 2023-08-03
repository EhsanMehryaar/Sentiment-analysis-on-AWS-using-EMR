# Import necessary libraries
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import sys
import boto3
import os
from datetime import datetime
import calendar
import random
import time
import simplejson

# Define the name of the Kinesis stream
stream_name = "tomz_test"

# Create a Kinesis client for the specified AWS region
kinesis_client = boto3.client('kinesis', region_name='us-east-1')

# Get Twitter API keys and access tokens from environment variables
consumerKey = os.environ["TWEET_CONSUMER_KEY"]
consumerSecret = os.environ["TWEET_CONSUMER_SECRET"]
accessToken = os.environ["TWITTER_ACCESS_TOKEN"]
accessSecret = os.environ["TWITTER_ACCESS_SECRET"]

# Define a custom StreamListener class that inherits from StreamListener
class Listener(StreamListener):
    # Method executed when new data (tweets) is received from the Twitter stream
    def on_data(self, data):
        # Print the received data (tweet) without trailing whitespace
        print(data.rstrip())
        
        # Send the tweet data to the specified Kinesis stream
        put_response = kinesis_client.put_record(
            StreamName=stream_name,
            Data=data,
            PartitionKey="fake"
        )
        
        # A small delay to prevent rate limiting issues (0.05 seconds)
        time.sleep(0.05)
        return True
    
    # Method executed when an error occurs during the Twitter stream connection
    def on_error(self, status):
        # Print the error status
        print(status)

# Set up Twitter API authentication using OAuthHandler
auth = OAuthHandler(consumerKey, consumerSecret)
auth.set_access_token(accessToken, accessSecret)

# Get tracked terms (keywords) from command-line arguments
track = sys.argv[1].split(',')

# Create a Twitter stream with the custom Listener class
twitterStream = Stream(auth, Listener())

# Start streaming tweets based on the tracked terms and English language
twitterStream.filter(track=track, language=["en"])
