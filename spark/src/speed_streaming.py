from pyspark.sql import SparkSession
from schema import schema
from spark_streaming_functions import *


# declare variables
kafka_url = "44.222.146.38:9092" # PUBLIC_IP:9092

listen_topic = "listen_events"
auth_topic = "auth_events"
page_view_topic = "page_view_events"
status_change_topic = "status_change_events"

redshift_url="jdbc:redshift://zingstreamp3-20240712.clwvssorqis4.us-east-1.redshift.amazonaws.com:5439/zingstreamp3_realtime"
redshift_properties = {
    "user": "huynhdoanho",
    "password": "DoanHo1112.",
    "driver": "com.amazon.redshift.jdbc42.Driver"
}


# create Spark Session
spark = SparkSession.builder \
    .appName("speed_streaming") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "2") \
    .config("spark.dynamicAllocation.maxExecutors", "2") \
    .getOrCreate()
#spark.streams.resetTerminated()


### auth events ###
auth_events_stream = read_stream_kafka(spark, kafka_url=kafka_url, topic=auth_topic) # read stream from kafka
auth_events_stream = process_stream(auth_events_stream, stream_schema=schema[auth_topic]) # process stream
write_auth_events_to_redshift = create_writer(redshift_url, redshift_properties, auth_topic) # write stream to redshift
auth_events_write_to_redshift = auth_events_stream.writeStream\
    .foreachBatch(write_auth_events_to_redshift)\
    .start()
###################

### listen events ###
listen_events_stream = read_stream_kafka(spark, kafka_url=kafka_url, topic=listen_topic) # read stream from kafka
listen_events_stream = process_stream(listen_events_stream, stream_schema=schema[listen_topic]) # process stream
write_listen_events_to_redshift = create_writer(redshift_url, redshift_properties, listen_topic) # write stream to redshift
listen_events_stream.writeStream\
    .foreachBatch(write_listen_events_to_redshift)\
    .start()#.awaitTermination()
###################

### page view events ###
page_view_events_stream = read_stream_kafka(spark, kafka_url=kafka_url, topic=page_view_topic) # read stream from kafka
page_view_events_stream = process_stream(page_view_events_stream, stream_schema=schema[page_view_topic]) # process stream
write_page_view_events_to_redshift = create_writer(redshift_url, redshift_properties, page_view_topic) # write stream to redshift
page_view_events_write_to_redshift = page_view_events_stream.writeStream\
    .foreachBatch(write_page_view_events_to_redshift)\
    .start()
###################

### status_change_events ###
status_change_events_stream = read_stream_kafka(spark, kafka_url=kafka_url, topic=status_change_topic) # read stream from kafka
status_change_events_stream = process_stream(status_change_events_stream, stream_schema=schema[status_change_topic]) # process stream
write_status_change_events_to_redshift = create_writer(redshift_url, redshift_properties, status_change_topic) # write stream to redshift
status_change_events_write_to_redshift = status_change_events_stream.writeStream\
    .foreachBatch(write_status_change_events_to_redshift)\
    .start()
###################


spark.streams.awaitAnyTermination()

