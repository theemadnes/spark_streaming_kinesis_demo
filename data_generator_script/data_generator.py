#!/usr/bin/python

# using some important bits from https://blogs.aws.amazon.com/bigdata/post/Tx3916WCIUPVA3T/Querying-Amazon-Kinesis-Streams-Directly-with-SQL-and-Spark-Streaming

# import critical modules
from __future__ import print_function # support python 2.7 & 3
import time
import datetime
import boto3
import random
import string
import calendar

# script will pull credentials & regions from AWSCLI
# otherwise define these variables
kinesis_stream = 'spark_streaming_kinesis_demo' # replace with your kinesis stream name - assumes that kinesis stream is in the same region as defined in CLI config
kinesis_client = boto3.client('kinesis')

# common username for kinesis records
user_name = 'alex'

def random_string_generator(size=6, chars=string.ascii_lowercase + string.digits):
    return ''.join(random.choice(chars) for x in range(size))

def random_int_generator():
    return random.randint(0, 999999)

def start():

    while True:

        # put together the payload for kinesis
        partition_key = random.choice('abcdefghij') # more partition keys distribute records across shards
        payload = user_name + ',' + random_string_generator() + ',' + str(random_int_generator()) + ',' + datetime.datetime.isoformat(datetime.datetime.now()) + ',' + str(int(time.time()))
        print(payload)

        # send this to kinesis
        result = kinesis_client.put_record(StreamName=kinesis_stream, Data=payload, PartitionKey=partition_key)
        print(result)

        time.sleep(.5)


if __name__ == "__main__":

    start()
