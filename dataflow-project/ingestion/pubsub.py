from google.cloud import pubsub_v1
import json
import time
from config.config import PROJECT_ID, TOPIC

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC)

def publish_orders():
    for i in range(1, 10):
        data = {
            "order_id": str(1000 + i),
            "price": 100 + i,
            "quantity": i
        }

        message = json.dumps(data).encode("utf-8")
        publisher.publish(topic_path, message)
        print(f"Published: {data}")

        time.sleep(2)

if __name__ == "__main__":
    publish_orders()