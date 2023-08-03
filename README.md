# Real-time Sentiment Analysis with Twitter and Amazon Kinesis using PySpark

## Overview
This project implements a real-time sentiment analysis system that streams tweets from Twitter API, performs sentiment analysis using either Amazon Comprehend or Amazon SageMaker, and stores the analyzed data in Amazon Kinesis. The sentiment analysis is performed on-the-fly using PySpark's Structured Streaming API, enabling continuous data processing and storage in a structured format.

## Features
- Real-time streaming of tweets from Twitter API based on specified keywords (tracked terms).
- Sentiment analysis using Amazon Comprehend or Amazon SageMaker for sentiment scoring.
- PySpark's Structured Streaming for processing and analyzing incoming data in real-time.
- Partitioning of data based on year, month, day, and hour for efficient storage and processing.
- Integration with AWS SDK (boto3) for seamless data transfer and interaction with AWS services.

## Requirements
- Python 3.x
- Tweepy
- PySpark
- boto3

## Setup and Configuration
1. Install the required libraries by running: pip install tweepy pyspark boto3
2. Set up Twitter API credentials:
- Create a Twitter Developer account and obtain API keys and access tokens.
- Set environment variables for the Twitter API keys in your system:
  ```
  export TWEET_CONSUMER_KEY=your_consumer_key
  export TWEET_CONSUMER_SECRET=your_consumer_secret
  export TWITTER_ACCESS_TOKEN=your_access_token
  export TWITTER_ACCESS_SECRET=your_access_secret
  ```

3. Set up AWS credentials:
- Create an AWS IAM user with appropriate permissions to access Kinesis and Comprehend/SageMaker.
- Configure AWS CLI or set environment variables with your AWS access key and secret key.

4. Configure the project:
- Modify the `stream_name`, `appName`, and other configurations in `kinesis_test_asl.py` as per your requirements.

## Running the Project
1. Start the Twitter streaming and data ingestion:
2. Start the real-time sentiment analysis:
    python kinesis_test_asl.py <app_name> <stream_name> <endpoint_url> <region_name> <interval> <format> <output_location>
    Replace the placeholders with appropriate values. For example: python kinesis_test_asl.py "TwitterSentimentAnalysis" "your_kinesis_stream"     "https://kinesis.us-east-1.amazonaws.com" "us-east-1" "60 seconds" "json" "/output_location"
3. Analyze the results:
- The sentiment scores will be continuously written to the specified output location in the structured format.
- You can use your preferred tools (e.g., AWS Glue, AWS Athena, or PySpark) to analyze and visualize the sentiment data.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE)
