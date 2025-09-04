from kafka import KafkaProducer
import json
import time
import random
from faker import Faker


fake= Faker()
# Change this to change the number of data that we are making up
Number_of_rows=1000
# Set up the conditions for Kafka to stream the data to a Kafka topic.
# Connects to the local Kafka broker and serializes messages as JSON
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer= lambda v:json.dumps(v).encode("utf-8")
)
def generate_athlete_event():
    #Generates a lime of fake data
    return {
        "athlete_id": random.randint(100000,999999),
        "name": random.choice(["100m","200m", "LongJump", "Marathon", "NA"]),
        "score": round(random.uniform(9.5,25.0),2),
        "region": random.choice(["EU","NA","ASIA"]),
        "timestamp":fake.iso8601()
    }

with open("stream.log","a") as f:
    # Generates the amount of data needed and then send it to Kafka
    # Also add each event to a local lof file for references
    # writes a line every 100ms
    for _ in range(Number_of_rows):

        event= generate_athlete_event()
        producer.send("athlete_events", value = event) # send to kafka
        print(f"Produced: {event}")
        f.write(json.dumps(event) +"\n") # Local writing
        time.sleep(0.1)

producer.flush()

producer.close() #closes producer
