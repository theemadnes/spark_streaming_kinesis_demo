# import critical modules
from __future__ import print_function # support python 2.7 & 3
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream

# Note: credentials will be pulled from IAM role assigned to EMR nodes. Make sure permissions are set properly for access to your Kinesis stream

# define variables
aws_region = 'us-west-2' # replace w/ AWS region used for Kinesis stream
kinesis_stream = 'spark_streaming_kinesis_demo' # replace with your Kinesis stream name
kinesis_endpoint = 'https://kinesis.' + aws_region + '.amazonaws.com' 
kinesis_app_name = 'alex_test_app'
kinesis_initial_position = InitialPositionInStream.LATEST # InitialPositionInStream.TRIM_HORIZON | InitialPositionInStream.LATEST
kinesis_checkpoint_interval = 2 # define how long to checkpoint when processing through the Kinesis stream
spark_batch_interval = 1 # 

# configure spark elements
spark_context = SparkContext(appName=kinesis_app_name)
# spark_streaming_context = StreamingContext(sc, 1) # sc valid for running in pyspark interactive mode
spark_streaming_context = StreamingContext(spark_context, spark_batch_interval)

kinesis_stream = KinesisUtils.createStream(
    spark_streaming_context, kinesis_app_name, kinesis_stream, kinesis_endpoint,
    aws_region, kinesis_initial_position, kinesis_checkpoint_interval) # previous example had ', StorageLevel.MEMORY_AND_DISK_2' at the end of the call

lines = kinesis_stream.map(lambda x: x[1])
counts = lines.flatMap(lambda line: line.split(",")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)
counts.pprint()
