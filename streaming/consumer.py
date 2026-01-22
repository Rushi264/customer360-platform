"""Kafka Consumer - Real-time Stream Processor"""
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text
import json, logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = 'localhost:9092'
DATABASE_URL = "postgresql://dataeng:password123@localhost:5432/customer360"

consumer = KafkaConsumer('clickstream_events', bootstrap_servers=KAFKA_BROKER, 
                        group_id='consumer_group', value_deserializer=lambda m: json.loads(m.decode('utf-8')))
engine = create_engine(DATABASE_URL)

def create_metrics_table():
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS real_time_metrics (
                metric_id SERIAL PRIMARY KEY,
                customer_id VARCHAR(20), event_type VARCHAR(50), event_count INTEGER,
                last_event_time TIMESTAMP, total_purchase_amount DECIMAL(10,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        conn.commit()

def process_event(event):
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT metric_id FROM real_time_metrics WHERE customer_id = :cid AND event_type = :etype"),
                                {"cid": event['customer_id'], "etype": event['event_type']})
            if result.fetchone():
                amount = event.get('amount', 0) if event['event_type'] == 'purchase' else 0
                conn.execute(text("""UPDATE real_time_metrics SET event_count = event_count + 1, 
                                    last_event_time = CURRENT_TIMESTAMP, 
                                    total_purchase_amount = COALESCE(total_purchase_amount, 0) + :amt
                                    WHERE customer_id = :cid AND event_type = :etype"""),
                            {"amt": amount, "cid": event['customer_id'], "etype": event['event_type']})
            else:
                amount = event.get('amount', 0) if event['event_type'] == 'purchase' else 0
                conn.execute(text("""INSERT INTO real_time_metrics (customer_id, event_type, event_count, total_purchase_amount)
                                    VALUES (:cid, :etype, 1, :amt)"""),
                            {"cid": event['customer_id'], "etype": event['event_type'], "amt": amount})
            conn.commit()
            logger.info(f"Processed: {event['event_type']} | {event['customer_id']}")
    except Exception as e:
        logger.error(f"Error: {e}")

def run_consumer():
    logger.info("CONSUMER STARTED")
    create_metrics_table()
    try:
        for message in consumer:
            process_event(message.value)
    except KeyboardInterrupt:
        logger.info("Stopped")
    finally:
        consumer.close()

if __name__ == "__main__":
    run_consumer()
