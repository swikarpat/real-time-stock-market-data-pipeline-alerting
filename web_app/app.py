from flask import Flask, render_template
from kafka import KafkaConsumer
import json
import threading
import time
import psycopg2
import os
import boto3

app = Flask(__name__)

# --- Configuration (Load from environment variables) ---
KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "stock_data_db")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")

# Kafka consumer for alerts (similar to alert_consumer)
consumer = KafkaConsumer(
    'stock-alerts',
    bootstrap_servers=KAFKA_BROKERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='web-app-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

alerts = []  # Store alerts here (in a real app, use a database)

def get_alerts_from_db():
    """Fetches alerts from the PostgreSQL database."""
    db_params = {
        "host": POSTGRES_HOST,
        "database": POSTGRES_DB,
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "port": POSTGRES_PORT
    }
    conn = None
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        cur.execute("SELECT symbol, alert_type, timestamp, message FROM alerts ORDER BY timestamp DESC LIMIT 10")
        alerts = cur.fetchall()
        cur.close()
        return alerts
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error fetching alerts from database: {error}")
        return []
    finally:
        if conn is not None:
            conn.close()

def consume_alerts():
    """Consumes alerts in a separate thread."""
    global alerts
    for message in consumer:
        alerts.insert(0, message.value)
        if len(alerts) > 10:
            alerts.pop()

# Start consuming alerts in a background thread
# threading.Thread(target=consume_alerts, daemon=True).start() # commented as we are reading from database

@app.route("/")
def index():
    """Displays the alerts."""
    latest_alerts = get_alerts_from_db()
    return render_template("index.html", alerts=latest_alerts)

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5001)