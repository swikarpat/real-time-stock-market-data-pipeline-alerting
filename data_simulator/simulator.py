import json
import time
import random
import logging
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError
from faker import Faker

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Kafka producer configuration
retries = 5
delay = 10
for attempt in range(retries):
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],  # Replace with your Kafka broker address if it's different
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5,  # Number of retries for producing a message
            acks='all'  # Ensure all brokers acknowledge message receipt
        )
        break  # Connection successful, break out of the retry loop
    except NoBrokersAvailable as e:
        logging.warning(f"Attempt {attempt + 1} failed: {e}")
        if attempt < retries - 1:
            logging.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)
        else:
            logging.error("Failed to connect to Kafka after multiple retries.")
            raise  # Re-raise the exception to stop the script

# Topic to send data to
topic_name = 'stock-market-data'

# Symbols for which to generate data
symbols = ['AAPL', 'MSFT', 'GOOG', 'AMZN', 'TSLA']

# Faker instance for generating realistic data
fake = Faker()

def generate_stock_data(symbol):
    """Generates realistic stock data for a given symbol."""
    price = round(random.uniform(50, 500), 2)  # Initial price range
    while True:
        # Simulate price fluctuations
        change_percent = random.uniform(-0.02, 0.02)  # Up to 2% change
        price = round(price * (1 + change_percent), 2)
        price = max(1, price)  # Keep the price above $1

        # Generate data point
        data = {
            'symbol': symbol,
            'timestamp': fake.date_time_between(start_date='-1m', end_date='now').isoformat(),
            'price': price,
            'volume': random.randint(100, 10000)
        }

        yield data

def main():
    """Generates and publishes stock data for multiple symbols."""
    for symbol in symbols:
        data_generator = generate_stock_data(symbol)
        for data in data_generator:
            try:
                future = producer.send(topic_name, value=data)
                record_metadata = future.get(timeout=10)  # Wait for acknowledgement with a timeout
                logging.info(f"Sent: {data} to partition {record_metadata.partition}, offset {record_metadata.offset}")
            except KafkaError as e:
                logging.error(f"Error sending data: {e}")
            time.sleep(random.uniform(0.1, 1))  # Simulate real-time data stream

if __name__ == "__main__":
    main()