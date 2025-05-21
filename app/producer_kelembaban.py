from confluent_kafka import Producer
import json
import time
import random

conf = {'bootstrap.servers': 'kafka:9092'}
producer = Producer(conf)

gudang_ids = ['G1', 'G2', 'G3']

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for message: {err}")
    else:
        # Decode message from bytes back to JSON string for display
        print(msg.value().decode('utf-8'))
        #print(f"Message delivered to {msg.topic()} partition [{msg.partition()}]")

try:
    while True:
        for gudang_id in gudang_ids:
            kelembaban = random.randint(60, 80)
            data = {"gudang_id": gudang_id, "kelembaban": kelembaban}
            producer.produce("sensor-kelembaban-gudang", json.dumps(data).encode('utf-8'), callback=delivery_report)
            producer.poll(0)
        time.sleep(1)
except KeyboardInterrupt:
    print("Producer kelembaban stopped.")
finally:
    producer.flush()
