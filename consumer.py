
from kafka import KafkaConsumer
import json
import pandas as pd
import time

records= []

# Change if you want a bigger batch 
batch_upload=10
# Set up a Kafka consumer to read the generated dara form the project
# "athletes_event"
# Connect to the local Kafka broker, reads messages from the beggining, and
# deserealized JSON
consumer = KafkaConsumer(
            "athlete_events",
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest', # start reading from the beginning of the topic
            group_id= 'consumer-group-1', # consumer group for tracking offsets
            value_deserializer= lambda v: json.loads(v.decode('utf-8')) 

)
total=0
# For every line in consumer it creates an event, which is the message value and append it to a list
for message in consumer:
    event= message.value
    records.append(event)
    print(f"Consumed: {event}")
    total+=1
    # For every batch of data it will upload it to the parquet. The bigger the batch the better for I/O cost and it will
    if len(records)>=batch_upload:
        Athletes_data= pd.DataFrame(records)
        Athletes_data.to_parquet(r"athletes_events.parquet", index= False) # create a local parquet with the DataFrame
        print(f"Batch of {len(records)}written to parquet")
        Athletes_data.head()
        records=[]

# If there is data left in the consumer, it appendes the leeft over batch to the parquet
if records:
    Athletes_data= pd.DataFrame(records)
    Athletes_data.to_parquet(r"curated\athletes_events.parquet", index= False)
    print(f"Batch of {len(records)}written to parquet")
    Athletes_data.head()

    records=[]
