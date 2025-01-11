import time
import json
import logging
import os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError
import boto3

# --- Configuration (Load from environment variables) ---
KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092")
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "stock-alerts")  # Add DynamoDB table configuration

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Kafka Producer Configuration with Retry Logic ---
retries = 5
delay = 10
for attempt in range(retries):
    try:
        alert_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5,
            acks='all'
        )
        break
    except NoBrokersAvailable:
        logging.warning(f"Attempt {attempt + 1} to connect to Kafka failed. Retrying in {delay} seconds...")
        if attempt < retries - 1:
            time.sleep(delay)
        else:
            logging.error("Failed to connect to Kafka after multiple retries.")
            raise

# Kafka topic for alerts
alert_topic_name = 'stock-alerts'

# --- DynamoDB Client ---
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(DYNAMODB_TABLE)

def read_data_from_database():
    """Reads aggregated data from PostgreSQL with retry logic."""
    retries = 3
    delay = 5
    for attempt in range(retries):
        try:
            with psycopg2.connect(**db_params) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT symbol, avg_price, total_volume, sma_5, window
                        FROM stock_aggs
                        WHERE (symbol, latest_timestamp) IN (
                            SELECT symbol, MAX(latest_timestamp)
                            FROM stock_aggs
                            GROUP BY symbol
                        )
                    """)
                    data = cur.fetchall()
                    logging.info("Successfully read data from database.")
                    return data
        except DBError as error:
            logging.warning(f"Attempt {attempt + 1} to read from database failed: {error}")
            if attempt < retries - 1:
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logging.error("Failed to read data from database after multiple retries.")
                raise

def check_for_alerts(data):
    """Checks for alert conditions based on the provided data."""
    for row in data:
        symbol, avg_price, total_volume, sma_5, window_time = row

        # Alert if price crosses above SMA
        if avg_price > sma_5:
            send_alert(symbol, 'PRICE_ABOVE_SMA', f'Price of {symbol} crossed above SMA: {avg_price} > {sma_5} at {window_time}')

def send_alert(symbol, alert_type, message):
    """Sends an alert message to the Kafka alerts topic and stores it in DynamoDB."""
    alert = {
        'symbol': symbol,
        'alert_type': alert_type,
        'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
        'message': message
    }

    try:
        alert_producer.send(alert_topic_name, value=alert)
        logging.info(f"Sent alert to Kafka: {alert}")
        store_alert_in_dynamodb(alert)
    except KafkaError as e:
        logging.error(f"Error sending alert to Kafka: {e}")

def store_alert_in_dynamodb(alert):
    """Stores the alert in DynamoDB."""
    try:
        response = table.put_item(Item=alert)
        logging.info(f"Alert stored in DynamoDB: {alert}")
    except Exception as e:
        logging.error(f"Error storing alert in DynamoDB: {e}")

def main():
    """Main loop to continuously check for alerts."""
    while True:
        data = read_data_from_database()
        check_for_alerts(data)
        time.sleep(60)

if __name__ == "__main__":
    main()