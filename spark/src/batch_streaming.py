from pyspark.sql import SparkSession
from schema import schema
from spark_streaming_functions import *
#import logging


# declare variables
kafka_url = "44.222.146.38:9092" # url = [public EC2_IP]:9092

listen_topic = "listen_events"
auth_topic = "auth_events"
page_view_topic = "page_view_events"
status_change_topic = "status_change_events"


# create Spark Session
spark = SparkSession.builder \
    .appName("batch_streaming") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "1") \
    .config("spark.dynamicAllocation.maxExecutors", "1") \
    .getOrCreate()
#spark.streams.resetTerminated()


# start streaming

### listen events ###
listen_events_stream = read_stream_kafka(spark, kafka_url=kafka_url, topic=listen_topic) # read stream from kafka
listen_events_stream = process_stream(listen_events_stream, stream_schema=schema[listen_topic]) # process stream
listen_events_write_stream = write_stream_file(listen_events_stream, topic=listen_topic) # write stream into file
#####################

### auth events ###
auth_events_stream = read_stream_kafka(spark, kafka_url=kafka_url, topic=auth_topic) # read stream from kafka
auth_events_stream = process_stream(auth_events_stream, stream_schema=schema[auth_topic]) # process stream
auth_events_write_stream = write_stream_file(auth_events_stream, topic=auth_topic) # write stream to file
#####################

### page view events ###
page_view_events_stream = read_stream_kafka(spark, kafka_url=kafka_url, topic=page_view_topic) # read stream from kafka
page_view_events_stream = process_stream(page_view_events_stream, stream_schema=schema[page_view_topic]) # process stream
page_view_events_write_stream = write_stream_file(page_view_events_stream, topic=page_view_topic) # write stream to file
#####################

### status_change_events ###
status_change_events_stream = read_stream_kafka(spark, kafka_url=kafka_url, topic=status_change_topic) # read stream from kafka
status_change_events_stream = process_stream(status_change_events_stream, stream_schema=schema[status_change_topic]) # process stream
status_change_events_write_stream = write_stream_file(status_change_events_stream, topic=status_change_topic)# write stream to file
#####################


listen_events_write_stream.start()
auth_events_write_stream.start()
page_view_events_write_stream.start()
status_change_events_write_stream.start()


spark.streams.awaitAnyTermination()
spark.stop()
