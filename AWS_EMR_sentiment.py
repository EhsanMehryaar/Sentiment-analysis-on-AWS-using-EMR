from __future__ import print_function
import sys, time, boto3
import simplejson as json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming .kinesis import KinesisUtils, InitialPositionStrean
from pyspark.sql import SpaarkSession
from pyspark.sql.types import IntegerType
from pyspaark.sql.function import udf, from_json, col, expr

def get_sentiment_comprehend(text):
    """retreive ssentiment from amazon comprehend

    Args:
        text (_type_): _description_

    Returns:
        _type_: _description_
    """    
    try:
        comprehend  = boto3.client(service_name='comprehend', region_name='us-east-1')
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

def get_sentiment_sagemaker(text):
    """using sagemaker for sentiment analysis

    Args:
        text (_type_): _description_

    Returns:
        _type_: _description_
    """    
    try:
        sagemaker = boto3.client('runtime.sagemaker', region_name='us-east-1')
        endpoint_name='sagemaker-mxnet-py2-cpu-01-21-37-29-539' #
        result = sagemaker.invoke_endpoint(
            EndpointName=endpoint_name,
            Body=json.dumps([text])
        )
        sentiment = json.loads(result["Body"].read())[0]
    except:
        sentiment = -99
    return sentiment

# user define function based on written functions
#func_udf = udf(get_sentiment_sagemaker, IntegerType())
func_udf = udf(get_sentiment_comprehend, IntegerType())

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print(
            "Usage: kinesis_test_asl.py <app_name> <stream-name> <stream-name> <endpoint-url> <region-name> <interval> <fomrat> <output-location>",
            file=sys.stderr)
        sys.exit(-1)
    
    appName = "Python Kinesis Structured Streaming Tweet Sentiments"
    interval = "60 seconds"
    
    streamName = "tomz-test"
    endpointUrl = "https://kinesis.us-east-1.amazonaws.com"
    regionName = "us-east-1"
    
    awsAccessKeyId = ""
    awsSecretKey = ""
    
    outputFormat = "json"
    outputLocation = ""
    
    # getting needed information from the commandline
    appName, streamName, endpointUrl, regionName, interval, outputFormat, outputLocation = sys.argv[1:]
    
    spark = SpaarkSession\
        .builder \
        .appName(appName)\
        .enableHiveSupport()\
        .getOrCreat()
        
    kinesisDf = spark \
        .readStream \
        .option("kinesis") \
        .option("streamName", streamName) \
        .option("initialPositio", "earliest") \
        .option("region", regionName) \
        .option("awsAccessKeyId", awsAccessKeyId) \
        .option("awsSecretKey", awsSecretKey) \
        .option("endpointUrl", endpointUrl) \
        .load
        
    # read sample json file
    tweets_sample = spark.read.json("") #
    
    json_schema = tweets_sample.schema # get the schema from sample json
    
    
    sentimentDF = kinesisDf \
        .selectExpr("cast (data as String) jsonData") \
        .select(from_json("jsonData", json_schema).alias("tweets")) \
        .select("tweets.*") \
        .withColumn('sentiment', func_udf(col('text'))) \
        .withColumn('year', expr('year(to_date(from_unixtime(timestamp_ms/1000)))')) \
        .withColumn('month', expr('month(to_date(from_unixtime(timestamp_ms/1000)))')) \
        .withColumn('day', expr('day(to_date(from_unixtime(timestamp_ms/1000)))')) \
        .withColumn('hour', expr('cast(from_unixtime(unix_timestamp(from_unixtime(timestamp_ms/1000), "yyyy-MM-dd HH:mm:ss", HH as int)')) \
        .writeStream \
        .partitionBy("year", "month", "day", "hour") \
        .format(outputFormat) \
        .option("checkpointLocation", "/outputCheckpoint") \
        .trigger(processingTime=interval) \
        .start(outputLocation)
        
    sentimentDF.awaitTermination()
    