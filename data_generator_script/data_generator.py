#!/usr/bin/python
# simulates a small IOT 'thing' to send random telemetry data to Kinesis
# pass a 'device name' when running the script as an argument, as this is part of the payload
# using some important bits from https://blogs.aws.amazon.com/bigdata/post/Tx3916WCIUPVA3T/Querying-Amazon-Kinesis-Streams-Directly-with-SQL-and-Spark-Streaming

# import critical modules
from __future__ import print_function # support python 2.7 & 3
import time
import datetime
import boto3
import random
import string
import calendar
import json
import sys

# script will pull credentials & regions from AWSCLI
# otherwise define these variables
kinesis_stream = 'blah12345-kinesisStream-MLIAMV07LW4Z' # replace with your kinesis stream name - assumes that kinesis stream is in the same region as defined in CLI config
kinesis_client = boto3.client('kinesis')

# set up the variables
device_name = 'null'
payload = {}

def random_string_generator(size=6, chars=string.ascii_lowercase + string.digits):
    return ''.join(random.choice(chars) for x in range(size))

def random_int_generator():
    return random.randint(0, 999999)

def start(argv):

    try:

        device_name = argv[0]

    except:

        print('USAGE: data_generator.py #YOUR_DEVICE_NAME#')
        sys.exit(2)

    while True:

        # put together the payload for kinesis
        partition_key = random.choice('abcdefghij') # more partition keys distribute records across shards
        payload['user_name'] = device_name
        payload['random_int'] = random_int_generator()
        payload['data_string'] = random_string_generator()
        payload['time_stamp'] = time.time() # will use this instead of Kinesis server-side timestamp

        print(payload)

        # send this to kinesis
        result = kinesis_client.put_record(StreamName=kinesis_stream, Data=json.dumps(payload), PartitionKey=partition_key)
        print(result)

        # time.sleep(.5) # do we need to sleep?


if __name__ == "__main__":

    start(sys.argv[1:])
