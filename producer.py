"""
producer.py
-----------
Assignment 2 - Streaming Data Analytics
Reads the sample e-commerce event dataset (generated from the Assignment 1
schema) and streams each event to the correct Kafka topic.

Topics used (must already exist - see create_topics.bat):
    pageview-events, cart-events, transaction-events, campaign-events

Usage:
    python producer.py
    python producer.py --events 100
    python producer.py --file data/sample_ecommerce_events.jsonl --delay 0.3
"""

import json
import argparse
import sys
import time
from pathlib import Path

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
except ImportError:
    print("ERROR: kafka-python is not installed.")
    print("Run:  pip install -r requirements.txt")
    sys.exit(1)

# Maps each GA4-style event_name to the Kafka topic it belongs to,
# matching the 4-topic design from Assignment 1.
TOPIC_MAP = {
    "campaign_click": "campaign-events",
    "page_view": "pageview-events",
    "view_item": "pageview-events",
    "add_to_cart": "cart-events",
    "begin_checkout": "cart-events",
    "purchase": "transaction-events",
}

DEFAULT_FILE = Path(__file__).parent / "data" / "sample_ecommerce_events.jsonl"


def load_events(file_path):
    """Load newline-delimited JSON events from disk."""
    events = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"  [WARN] Skipping malformed line {line_num}: {e}")
    return events


def build_producer(bootstrap_servers):
    """Create and return a configured KafkaProducer, or exit with a clear error."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
            acks="all",              # wait for broker acknowledgement -> reliable demo
            retries=3,
            request_timeout_ms=15000,
        )
        return producer
    except Exception as e:
        print(f"ERROR: Unexpected error while creating Kafka producer: {e}")
        sys.exit(1)


def send_event(producer, event, event_index):
    """Send a single event to its mapped topic, using ga_session_id as the key."""
    event_name = event.get("event_name", "unknown")
    topic = TOPIC_MAP.get(event_name)

    if topic is None:
        print(f"  [WARN] #{event_index}: unrecognised event_name '{event_name}', skipping.")
        return False

    # ga_session_id is used as the Kafka message key. This keeps every event
    # belonging to the same browsing session on the same partition, in the
    # order they were produced, which matters for later session-level
    # aggregation (conversion rate, cart abandonment) in Assignment 3.
    key = event.get("ga_session_id")

    try:
        future = producer.send(topic, key=key, value=event)
        record_metadata = future.get(timeout=10)  # block until ack, for clear demo output
        print(
            f"  [SENT] #{event_index} -> topic={record_metadata.topic} "
            f"partition={record_metadata.partition} offset={record_metadata.offset} "
            f"| event={event_name} key={key} "
            f"user={event.get('user_pseudo_id')}"
        )
        return True
    except KafkaError as e:
        print(f"  [ERROR] #{event_index}: failed to send event ({event_name}) - {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Stream sample e-commerce events to Kafka.")
    parser.add_argument("--file", type=str, default=str(DEFAULT_FILE),
                         help="Path to the JSONL sample data file")
    parser.add_argument("--events", type=int, default=None,
                         help="Limit the number of events to send (default: send all)")
    parser.add_argument("--broker", type=str, default="localhost:9092",
                         help="Kafka bootstrap server (default: localhost:9092)")
    parser.add_argument("--delay", type=float, default=0.2,
                         help="Seconds to wait between messages, to simulate a live stream (default: 0.2)")
    args = parser.parse_args()

    file_path = Path(args.file)
    if not file_path.exists():
        print(f"ERROR: Data file not found: {file_path}")
        print("Run 'python data/generate_data.py' first to create the sample dataset.")
        sys.exit(1)

    print(f"Loading events from: {file_path}")
    events = load_events(file_path)
    if args.events is not None:
        events = events[: args.events]
    print(f"Loaded {len(events)} events to send.\n")

    print(f"Connecting to Kafka broker at {args.broker} ...")
    producer = build_producer(args.broker)
    print("Connected. Streaming events...\n")

    sent_count = 0
    failed_count = 0

    try:
        for i, event in enumerate(events, start=1):
            success = send_event(producer, event, i)
            if success:
                sent_count += 1
            else:
                failed_count += 1
            if args.delay > 0:
                time.sleep(args.delay)
    except KeyboardInterrupt:
        print("\nInterrupted by user. Flushing what has been sent so far...")
    finally:
        producer.flush()
        producer.close()

    print("\n----------------------------------------")
    print(f"Done. {sent_count} events sent successfully, {failed_count} failed.")
    print("----------------------------------------")


if __name__ == "__main__":
    main()
