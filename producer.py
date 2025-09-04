from kafka import KafkaProducer
import json
import time
import random
from faker import Faker


fake= Faker()
# Change this to change the number of data that we are making up
Number_of_rows=1000
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer= lambda v:json.dumps(v).encode("utf-8")
)
def generate_athlete_event():
    return {
        "athlete_id": random.randint(100000,999999),
        "name": random.choice(["100m","200m", "LongJump", "Marathon", "NA"]),
        "score": round(random.uniform(9.5,25.0),2),
        "region": random.choice(["EU","NA","ASIA"]),
        "timestamp":fake.iso8601()
    }

with open("stream.log","a") as f:
    for _ in range(Number_of_rows):

        event= generate_athlete_event()
        producer.send("athlete_events", value = event)
        print(f"Produced: {event}")
        f.write(json.dumps(event) +"\n")
        time.sleep(0.1)

producer.flush()
producer.close()