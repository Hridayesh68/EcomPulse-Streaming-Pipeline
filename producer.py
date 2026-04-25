# producer.py
from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

products = ["Laptop", "Phone", "Shoes", "Watch", "Headphones"]

while True:
    event = {
        "user_id": random.randint(1, 100),
        "product": random.choice(products),
        "price": round(random.uniform(100, 2000), 2),
        "event_type": random.choice(["view", "cart", "purchase"]),
        "timestamp": time.time()
    }

    producer.send("ecommerce_topic", event)
    print("Sent:", event)

    time.sleep(1)
