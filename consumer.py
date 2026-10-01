"""
consumer.py
-----------
Assignment 3 - Streaming Data Analytics
Continuously reads events from all four Kafka topics created in Assignment 2
(pageview-events, cart-events, transaction-events, campaign-events) and
writes each event into the PostgreSQL 'events' table, which Grafana then
queries to render the live dashboard.

Usage:
    python consumer.py

Leave this running - it will keep listening and writing new events until
you stop it with Ctrl+C. Run it BEFORE (or at the same time as) producer.py
from Assignment 2, so it is actively listening when events are published.
"""

import json
import sys
import signal

try:
    from kafka import KafkaConsumer
except ImportError:
    print("ERROR: kafka-python is not installed.")
    print("Run:  pip install kafka-python==3.0.11")
    sys.exit(1)

try:
    import psycopg2
except ImportError:
    print("ERROR: psycopg2-binary is not installed.")
    print("Run:  pip install psycopg2-binary")
    sys.exit(1)

# ---- Configuration (matches Assignment 2 topics and the Postgres setup) ----
TOPICS = ["pageview-events", "cart-events", "transaction-events", "campaign-events"]
KAFKA_BROKER = "localhost:9092"

DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "sda_analytics"
DB_USER = "sda_user"
DB_PASSWORD = "sda_pass"

INSERT_SQL = """
INSERT INTO events (
    topic, event_timestamp, event_name, user_pseudo_id, ga_session_id,
    item_id, item_name, item_category, price, quantity,
    traffic_source_source, traffic_source_medium, traffic_source_name,
    device_category, device_os, geo_country, geo_city,
    order_id, transaction_value
) VALUES (
    %(topic)s, %(event_timestamp)s, %(event_name)s, %(user_pseudo_id)s, %(ga_session_id)s,
    %(item_id)s, %(item_name)s, %(item_category)s, %(price)s, %(quantity)s,
    %(traffic_source_source)s, %(traffic_source_medium)s, %(traffic_source_name)s,
    %(device_category)s, %(device_os)s, %(geo_country)s, %(geo_city)s,
    %(order_id)s, %(transaction_value)s
);
"""


def event_to_row(topic, event):
    """Flatten a nested GA4-style JSON event into a row dict matching the events table."""
    traffic = event.get("traffic_source", {}) or {}
    device = event.get("device", {}) or {}
    geo = event.get("geo", {}) or {}

    return {
        "topic": topic,
        "event_timestamp": event.get("event_timestamp"),
        "event_name": event.get("event_name"),
        "user_pseudo_id": event.get("user_pseudo_id"),
        "ga_session_id": event.get("ga_session_id"),
        "item_id": event.get("item_id"),
        "item_name": event.get("item_name"),
        "item_category": event.get("item_category"),
        "price": event.get("price"),
        "quantity": event.get("quantity"),
        "traffic_source_source": traffic.get("source"),
        "traffic_source_medium": traffic.get("medium"),
        "traffic_source_name": traffic.get("name"),
        "device_category": device.get("category"),
        "device_os": device.get("operating_system"),
        "geo_country": geo.get("country"),
        "geo_city": geo.get("city"),
        "order_id": event.get("order_id"),
        "transaction_value": event.get("transaction_value"),
    }


def build_consumer():
    try:
        return KafkaConsumer(
            *TOPICS,
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k is not None else None,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="sda-assignment3-consumer",
        )
    except Exception as e:
        print(f"ERROR: Could not connect to Kafka broker at {KAFKA_BROKER}: {e}")
        sys.exit(1)


def build_db_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        conn.autocommit = True
        return conn
    except Exception as e:
        print(f"ERROR: Could not connect to PostgreSQL at {DB_HOST}:{DB_PORT}: {e}")
        print("Make sure the sda_postgres Docker container is running (docker ps).")
        sys.exit(1)


def main():
    print(f"Connecting to Kafka broker at {KAFKA_BROKER} (topics: {', '.join(TOPICS)}) ...")
    consumer = build_consumer()

    print(f"Connecting to PostgreSQL at {DB_HOST}:{DB_PORT}/{DB_NAME} ...")
    conn = build_db_connection()
    cur = conn.cursor()

    print("Connected. Listening for events... (Ctrl+C to stop)\n")

    count = 0
    running = True

    def handle_sigint(sig, frame):
        nonlocal running
        print("\nStopping consumer (finishing current message)...")
        running = False

    signal.signal(signal.SIGINT, handle_sigint)

    try:
        for message in consumer:
            if not running:
                break
            event = message.value
            row = event_to_row(message.topic, event)
            try:
                cur.execute(INSERT_SQL, row)
                count += 1
                print(
                    f"  [STORED] #{count} topic={message.topic} partition={message.partition} "
                    f"offset={message.offset} | event={row['event_name']} "
                    f"session={row['ga_session_id']} user={row['user_pseudo_id']}"
                )
            except Exception as e:
                print(f"  [ERROR] Failed to insert event into Postgres: {e}")
    finally:
        cur.close()
        conn.close()
        consumer.close()
        print("\n----------------------------------------")
        print(f"Consumer stopped. {count} events written to PostgreSQL.")
        print("----------------------------------------")


if __name__ == "__main__":
    main()
