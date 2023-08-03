# Import necessary libraries
from __future__ import print_function
import sys, time, boto3
import simplejson as json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionStrean
from pyspark.sql import SpaarkSession
from pyspark.sql.types import IntegerType
from pyspaark.sql.function import udf, from_json, col, expr

# Define a function to retrieve sentiment using Amazon Comprehend
def get_sentiment_comprehend(text):
    """Retrieve sentiment from Amazon Comprehend

    Args:
        text (str): The text to analyze the sentiment.

    Returns:
        int: The sentiment score (-99 for error, 0 for neutral, 1 for positive, -1 for negative).
    """    
    try:
        comprehend = boto3.client(service_name='comprehend', region_name='us-east-1')
        response = comprehend.detect_sentiment(Text=text, LanguageCode='en')
        sentiment_str = response["Sentiment"]
        if sentiment_str == "POSITIVE":
            sentiment = 1
        elif sentiment_str == "NEGATIVE":
            sentiment = -1
        elif sentiment_str == "NEUTRAL":
            sentiment = 0
        else:
            sentiment = -99
    except:
        sentiment = -99
    return sentiment

# Define a function to retrieve sentiment using Amazon SageMaker
def get_sentiment_sagemaker(text):
    """Using SageMaker for sentiment analysis

    Args:
        text (str): The text to analyze the sentiment.

    Returns:
        int: The sentiment score (-99 for error, 0 for neutral, 1 for positive, -1 for negative).
    """    
    try:
        sagemaker = boto3.client('runtime.sagemaker', region_name='us-east-1')
        endpoint_name = 'sagemaker-mxnet-py2-cpu-01-21-37-29-539' # SageMaker endpoint name
        result = sagemaker.invoke_endpoint(
            EndpointName=endpoint_name,
            Body=json.dumps([text])
        )
        sentiment = json.loads(result["Body"].read())[0]
    except:
        sentiment = -99
    return sentiment

# Define a user-defined function based on the selected sentiment function
func_udf = udf(get_sentiment_comprehend, IntegerType())

# Check if the script is being executed as the main program
if __name__ == "__main__":
    # Check if the correct number of command-line arguments is provided
    if len(sys.argv) != 5:
        print(
            "Usage: kinesis_test_asl.py <app_name> <stream-name> <stream-name> <endpoint-url> <region-name> <interval> <format> <output-location>",
            file=sys.stderr)
        sys.exit(-1)
    
    # Extract command-line arguments for configuration
    appName, streamName, endpointUrl, regionName, interval, outputFormat, outputLocation = sys.argv[1:]
    
    # Create a Spark session with Hive support
    spark = SparkSession.builder \
        .appName(appName) \
        .enableHiveSupport() \
        .getOrCreate()
    
    # Read data from the Kinesis stream using Spark Structured Streaming
    kinesisDf = spark \
        .readStream \
        .option("kinesis", "True") \
        .option("streamName", streamName) \
        .option("initialPosition", "earliest") \
        .option("region", regionName) \
        .option("awsAccessKeyId", awsAccessKeyId) \
        .option("awsSecretKey", awsSecretKey) \
        .option("endpointUrl", endpointUrl) \
        .load()
    
    # Read a sample JSON file to get the schema
    tweets_sample = spark.read.json("") #
    
    # Get the schema from the sample JSON
    json_schema = tweets_sample.schema
    
    # Perform sentiment analysis on the streaming data
    sentimentDF = kinesisDf \
        .selectExpr("cast(data as String) jsonData") \
        .select(from_json("jsonData", json_schema).alias("tweets")) \
        .select("tweets.*") \
        .withColumn('sentiment', func_udf(col('text'))) \
        .withColumn('year', expr('year(to_date(from_unixtime(timestamp_ms/1000)))')) \
        .withColumn('month', expr('month(to_date(from_unixtime(timestamp_ms/1000)))')) \
        .withColumn('day', expr('day(to_date(from_unixtime(timestamp_ms/1000)))')) \
        .withColumn('hour', expr('cast(from_unixtime(unix_timestamp(from_unixtime(timestamp_ms/1000), "yyyy-MM-dd HH:mm:ss"), HH as int)')) \
        .writeStream \
        .partitionBy("year", "month", "day", "hour") \
        .format(outputFormat) \
        .option("checkpointLocation", "/outputCheckpoint") \
        .trigger(processingTime=interval) \
        .start(outputLocation)
    
    # Wait for the streaming job to terminate
    sentimentDF.awaitTermination()
