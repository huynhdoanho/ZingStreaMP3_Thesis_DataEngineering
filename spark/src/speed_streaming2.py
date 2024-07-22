from pyspark.sql import SparkSession
from schema import schema
from spark_streaming_functions import *

spark = SparkSession.builder \
    .appName("Kafka") \
    .master("spark://spark-master:7077") \
    .getOrCreate()


#spark.streams.resetTerminated()

kafka_url = "3.236.201.8:9092" # private ip : 9092

listen_topic = "listen_events"
auth_topic = "auth_events"
page_view_topic = "page_view_events"


postgres_url="jdbc:redshift://zingstreamp3-20240608.clwvssorqis4.us-east-1.redshift.amazonaws.com:5439/realtime"
postgres_properties = {
    "user": "huynhdoanho",
    "password": "DoanHo1112.",
    "driver": "com.amazon.redshift.jdbc42.Driver"#"org.postgresql.Driver"
}


### auth events ###
# read stream from kafka
auth_events_stream = read_stream_kafka(spark, kafka_url=kafka_url, topic=auth_topic)

# process stream
auth_events_stream = process_stream(auth_events_stream, stream_schema=schema[auth_topic])

# write stream to postgres
write_auth_events_to_postgres = create_writer(postgres_url, postgres_properties, auth_topic)

auth_events_write_to_postgres = auth_events_stream.writeStream\
    .foreachBatch(write_auth_events_to_postgres)\
    .start()


### listen events ###
# read stream from kafka
listen_events_stream = read_stream_kafka(spark, kafka_url=kafka_url, topic=listen_topic)

# process stream
listen_events_stream = process_stream(listen_events_stream, stream_schema=schema[listen_topic])

# write stream to postgres
write_listen_events_to_postgres = create_writer(postgres_url, postgres_properties, listen_topic)

#listen_events_write_to_postgres = 
listen_events_stream.writeStream\
    .foreachBatch(write_listen_events_to_postgres).start()





### page view events ###
# read stream from kafka
page_view_events_stream = read_stream_kafka(spark, kafka_url=kafka_url, topic=page_view_topic)

# process stream
page_view_events_stream = process_stream(page_view_events_stream, stream_schema=schema[page_view_topic])

# write stream to postgres
write_page_view_events_to_postgres = create_writer(postgres_url, postgres_properties, page_view_topic)

page_view_events_write_to_postgres = page_view_events_stream.writeStream\
    .foreachBatch(write_page_view_events_to_postgres)\
    .start()


 


# ##########
# status_change_topic = "status_change_events"
# ### status_change_events ###
# # read stream from kafka
# status_change_events_stream = read_stream_kafka(spark, kafka_url=kafka_url, topic=status_change_topic)

# # process stream
# status_change_events_stream = process_stream(status_change_events_stream, stream_schema=schema[status_change_topic])

# # write stream to file
# # status_change_events_write_stream = write_stream_file(status_change_events_stream, topic=status_change_topic)
# # status_change_events_write_stream.start()

# # write stream to postgres
# write_status_change_events_to_postgres = create_writer(postgres_url, postgres_properties, status_change_topic)

# status_change_events_write_to_postgres = status_change_events_stream.writeStream\
#     .foreachBatch(write_status_change_events_to_postgres)\
#     .start()


spark.streams.awaitAnyTermination()

