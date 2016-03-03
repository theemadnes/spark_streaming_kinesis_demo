# spark_streaming_kinesis_demo

This is a simple demo project that combines AWS Kinesis + Spark to demonstrate the conversion of streaming JSON to CSV, and transform the epoch time to an ISO timestamp, finally storing to S3.

The data generator script will send random 'telemetry' data to Kinesis. When executing, pass a 'device name' as an argument.

When launching the spark script, make sure to link the appropriate Kinesis + Spark jar(s), as this is needed for Kinesis integration.
