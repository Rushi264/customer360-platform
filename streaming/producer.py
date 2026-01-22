"""Kafka Producer - Clickstream Event Generator"""
from kafka import KafkaProducer
from datetime import datetime
import json, random, time, logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'clickstream_events'
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

CUSTOMERS = [f"CUST_{i:05d}" for i in range(1, 101)]
PRODUCTS = [f"PROD_{i:04d}" for i in range(1, 51)]

def generate_event():
    event = {
        'event_type': random.choice(['page_view', 'click', 'purchase']),
        'customer_id': random.choice(CUSTOMERS),
        'timestamp': datetime.now().isoformat(),
    }
    if event['event_type'] == 'purchase':
        event['product_id'] = random.choice(PRODUCTS)
        event['amount'] = round(random.uniform(20, 1000), 2)
    return event

def run_producer(num_events=100):
    logger.info("PRODUCER STARTED")
    try:
        for i in range(num_events):
            event = generate_event()
            future = producer.send(KAFKA_TOPIC, value=event)
            future.get(timeout=10)
            logger.info(f"[{i+1}/{num_events}] {event['event_type']} | {event['customer_id']}")
            time.sleep(0.5)
        logger.info(f"All {num_events} events sent!")
    finally:
        producer.close()

if __name__ == "__main__":
    run_producer(100)
