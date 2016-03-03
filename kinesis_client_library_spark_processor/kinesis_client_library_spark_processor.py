### this sample program takes data pulled from a Kinesis stream, in JSON format, and converts to CSV and saves to S3
### when executing from the CLI using

# import critical modules
from __future__ import print_function # support python 2.7 & 3
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream
import datetime
import json

# Note: credentials will be pulled from IAM role assigned to EMR nodes. Make sure permissions are set properly for access to your Kinesis stream

# define variables
s3_target_bucket_name = 'mattsona-spark-demo' # replace with your bucket name for target data
aws_region = 'us-west-2' # replace w/ AWS region used for Kinesis stream
kinesis_stream = 'spark_streaming_kinesis_demo' # replace with your Kinesis stream name
kinesis_endpoint = 'https://kinesis.' + aws_region + '.amazonaws.com' # the public endpiont of the AWS region this is executed from
kinesis_app_name = 'alex_test_app' # app name used to track process through the Kinesis stream
kinesis_initial_position = InitialPositionInStream.LATEST # InitialPositionInStream.TRIM_HORIZON | InitialPositionInStream.LATEST
kinesis_checkpoint_interval = 10 # define how long to checkpoint when processing through the Kinesis stream
spark_batch_interval = 10 # how many seconds before pulling the next batch of data from the Kinesis stream

# configure spark elements
spark_context = SparkContext(appName=kinesis_app_name)
# spark_streaming_context = StreamingContext(sc, 1) # sc valid for running in pyspark interactive mode
spark_streaming_context = StreamingContext(spark_context, spark_batch_interval)

kinesis_stream = KinesisUtils.createStream(
    spark_streaming_context, kinesis_app_name, kinesis_stream, kinesis_endpoint,
    aws_region, kinesis_initial_position, kinesis_checkpoint_interval) # previous example had ', StorageLevel.MEMORY_AND_DISK_2' at the end of the call

# take kinesis stream JSON data and convert to CSV # just realized we're still dealing with dstreams, not RDD, so naming is inaccurate
py_dict_rdd = kinesis_stream.map(lambda x: json.loads(x))
# need to convert int (time_stamp & random_int) to string
csv_rdd = py_dict_rdd.map(lambda x: x['user_name'] + ',' + str(datetime.datetime.utcfromtimestamp(x['time_stamp'])) + ',' + x['data_string'] + ',' + str(x['random_int']))

# save that rdd to S3
commit_to_s3 = csv_rdd.saveAsTextFiles('s3://' + s3_target_bucket_name + '/spark_streaming_processing/ '+ datetime.datetime.isoformat(datetime.datetime.now()).replace(':','_'))
# commit_to_s3 = kinesis_stream.saveAsTextFiles('s3://mattsona-public/' + datetime.datetime.isoformat(datetime.datetime.now()).replace(':','_'))

spark_streaming_context.start()

spark_streaming_context.awaitTermination()
