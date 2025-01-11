from kafka import KafkaConsumer
import json
import logging
import os

# --- Configuration (Load from environment variables) ---
KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092")

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Kafka consumer configuration
consumer = KafkaConsumer(
    'stock-alerts',
    bootstrap_servers=KAFKA_BROKERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def send_notification(alert):
    """Sends a notification for the given alert."""
    logging.info(f"ALERT: {alert['symbol']} - {alert['alert_type']}: {alert['message']}")

def main():
    """Consumes alerts from the Kafka topic and sends notifications."""
    for message in consumer:
        alert = message.value
        send_notification(alert)

if __name__ == "__main__":
    main()