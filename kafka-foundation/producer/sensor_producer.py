import json
import os
import random
import time
from datetime import datetime

from kafka import KafkaProducer

TOPIC = "iot_sensors"
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

sensor_ids = ["sensor-1", "sensor-2", "sensor-3"]

print(f"Sending messages to topic '{TOPIC}' on {BOOTSTRAP_SERVERS} ... Press CTRL+C to stop.")
try:
    while True:
        msg = {
            "sensor_id": random.choice(sensor_ids),
            "temperature": round(random.uniform(18.0, 35.0), 2),
            "humidity": round(random.uniform(30.0, 80.0), 2),
            "timestamp": datetime.utcnow().isoformat(),
        }

        producer.send(TOPIC, value=msg)
        producer.flush()

        print("Sent:", msg)
        time.sleep(1)

except KeyboardInterrupt:
    print("\nStopped by user.")
finally:
    producer.close()
