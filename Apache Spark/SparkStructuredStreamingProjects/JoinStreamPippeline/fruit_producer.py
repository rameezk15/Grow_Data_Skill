import time
import json
from confluent_kafka import Producer

# Prepare the producer
p = Producer({'bootstrap.servers': 'localhost:9092'})

# Load the JSON data
with open('fruit_data.json') as f:
    data = json.load(f)

# Publish the data to the Kafka topic
for row in data:
    p.produce('fruit_data', json.dumps(row))
    p.flush()
    print("Message published -> ", json.dumps(row))
    time.sleep(2)  # delay between messages
