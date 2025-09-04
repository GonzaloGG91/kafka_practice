
from kafka import KafkaConsumer
import json
import pandas as pd
import time

records= []

# Change if you want a bigger batch 
batch_upload=10
# Download from the dockers the data that has been created
consumer = KafkaConsumer(
            "athlete_events",
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            group_id= 'consumer-group-1',
            value_deserializer= lambda v: json.loads(v.decode('utf-8'))

)
total=0

for message in consumer:
    event= message.value
    records.append(event)
    print(f"Consumed: {event}")
    total+=1
    # For every batch of data it will upload it to the parquet. The bigger the batch the better for I/O cost and it will
    if len(records)>=batch_upload:
        Athletes_data= pd.DataFrame(records)
        Athletes_data.to_parquet(r"athletes_events.parquet", index= False)
        print(f"Batch of {len(records)}written to parquet")
        Athletes_data.head()
        records=[]


if records:
    Athletes_data= pd.DataFrame(records)
    Athletes_data.to_parquet(r"curated\athletes_events.parquet", index= False)
    print(f"Batch of {len(records)}written to parquet")
    Athletes_data.head()
    records=[]