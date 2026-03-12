# ──────────────────────────────────────────────────
# Author: Naveen Vishlavath
# File: producer/event_producer.py
#
# Optimized Kafka event producer for e-commerce platform.
# Improvements over v1:
#   - All config loaded from .env file
#   - Dead letter queue for failed events
#   - Schema validation before sending
#   - Delivery callbacks for every message
#   - Graceful shutdown handling
# ──────────────────────────────────────────────────

import json
import time
import random
import logging
import os
import signal
import sys
from datetime import datetime
from typing import Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError
from faker import Faker
from dotenv import load_dotenv

# load environment variables from .env file
# this way no hardcoded config in the code
load_dotenv()

# ── logging setup ──────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ── config from environment variables ──────────────
# using defaults as fallback in case .env is missing
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC             = os.getenv('KAFKA_TOPIC', 'ecommerce-events')
KAFKA_RETRIES           = int(os.getenv('KAFKA_RETRIES', 3))
KAFKA_ACKS              = os.getenv('KAFKA_ACKS', 'all')
SLEEP_INTERVAL          = float(os.getenv('PRODUCER_SLEEP_INTERVAL', 1))
FLUSH_INTERVAL          = int(os.getenv('PRODUCER_FLUSH_INTERVAL', 10))

# dead letter topic — failed events go here for investigation
# this is a real production pattern used at most data companies
DEAD_LETTER_TOPIC = f"{KAFKA_TOPIC}-dead-letter"

# ── faker instance ──────────────────────────────────
fake = Faker()

# ── event types with realistic traffic weights ──────
# item_viewed is most frequent — mirrors real e-commerce traffic
EVENT_TYPES   = ['order_placed', 'item_viewed', 'cart_updated', 'payment_processed']
EVENT_WEIGHTS = [20, 50, 20, 10]

# ── product catalog ─────────────────────────────────
PRODUCTS = [
    {'product_id': 'P001', 'name': 'Wireless Headphones', 'price': 79.99, 'category': 'Electronics'},
    {'product_id': 'P002', 'name': 'Running Shoes',        'price': 59.99, 'category': 'Sports'},
    {'product_id': 'P003', 'name': 'Coffee Maker',         'price': 49.99, 'category': 'Kitchen'},
    {'product_id': 'P004', 'name': 'Yoga Mat',             'price': 29.99, 'category': 'Sports'},
    {'product_id': 'P005', 'name': 'Laptop Stand',         'price': 39.99, 'category': 'Electronics'},
    {'product_id': 'P006', 'name': 'Water Bottle',         'price': 19.99, 'category': 'Sports'},
    {'product_id': 'P007', 'name': 'Desk Lamp',            'price': 34.99, 'category': 'Home'},
    {'product_id': 'P008', 'name': 'Backpack',             'price': 69.99, 'category': 'Fashion'},
]


# ── schema validation ───────────────────────────────
def validate_event(event: dict) -> bool:
    """
    Validate event has all required fields before sending.
    Catches bad data early — much cheaper than fixing it
    downstream in Spark or dbt.
    """
    required_fields = [
        'event_id', 'event_type', 'user_id',
        'session_id', 'timestamp', 'product_id', 'price'
    ]
    for field in required_fields:
        if field not in event or event[field] is None:
            logger.error(f"❌ Validation failed — missing field: {field}")
            return False

    if event['event_type'] not in EVENT_TYPES:
        logger.error(f"❌ Unknown event type: {event['event_type']}")
        return False

    if event['price'] <= 0:
        logger.error(f"❌ Invalid price: {event['price']}")
        return False

    return True


# ── event generator ─────────────────────────────────
def generate_event(event_type: str) -> dict:
    """
    Generate a realistic e-commerce event.
    Each event type has its own specific fields
    mirroring how real e-commerce systems work.
    """
    product  = random.choice(PRODUCTS)
    quantity = random.randint(1, 5)

    event = {
        'event_id':     fake.uuid4(),
        'event_type':   event_type,
        'user_id':      random.randint(1000, 9999),
        'session_id':   fake.uuid4(),
        'timestamp':    datetime.utcnow().isoformat(),
        'product_id':   product['product_id'],
        'product_name': product['name'],
        'category':     product['category'],
        'price':        product['price'],
    }

    if event_type in ('order_placed', 'payment_processed'):
        event['quantity']     = quantity
        event['total_amount'] = round(product['price'] * quantity, 2)

    if event_type == 'cart_updated':
        event['action']   = random.choice(['add', 'remove'])
        event['quantity'] = quantity

    if event_type == 'payment_processed':
        event['payment_method'] = random.choice([
            'credit_card', 'debit_card', 'paypal', 'apple_pay'
        ])
        # weighted towards success — realistic payment success rate ~90%
        event['status'] = random.choices(
            ['success', 'failed'],
            weights=[90, 10]
        )[0]

    return event


# ── delivery callbacks ───────────────────────────────
def on_send_success(record_metadata):
    """Called automatically when Kafka confirms message delivery."""
    logger.debug(
        f"✅ Delivered to topic={record_metadata.topic} "
        f"partition={record_metadata.partition} "
        f"offset={record_metadata.offset}"
    )


def on_send_error(producer: KafkaProducer, event: dict):
    """
    Called when message delivery fails.
    Sends failed event to dead letter topic instead of losing it.
    This is a critical production pattern.
    """
    def handler(excp):
        logger.error(f"❌ Failed to deliver event: {excp}")
        try:
            producer.send(DEAD_LETTER_TOPIC, value=event)
            logger.info(f"📨 Event sent to dead letter topic: {DEAD_LETTER_TOPIC}")
        except Exception as e:
            logger.error(f"❌ Could not send to dead letter topic: {e}")
    return handler


# ── kafka producer factory ───────────────────────────
def create_kafka_producer(retries: int = 5) -> KafkaProducer:
    """
    Create Kafka producer with retry logic.
    Retries are important because Kafka container might
    still be starting up when this script runs.
    """
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks=KAFKA_ACKS,
                retries=KAFKA_RETRIES,
                # wait max 30 seconds for metadata
                max_block_ms=30000,
                # compress messages to save bandwidth
                compression_type='gzip',
            )
            logger.info(f"✅ Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except Exception as e:
            wait = (attempt + 1) * 5
            logger.warning(
                f"Attempt {attempt + 1}/{retries} failed. "
                f"Retrying in {wait}s... Error: {e}"
            )
            time.sleep(wait)

    raise Exception("❌ Could not connect to Kafka after all attempts")


# ── graceful shutdown ────────────────────────────────
# global flag to handle Ctrl+C cleanly
running = True

def handle_shutdown(signum, frame):
    """Handle Ctrl+C or kill signal gracefully."""
    global running
    logger.info("\n⛔ Shutdown signal received...")
    running = False

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


# ── main loop ────────────────────────────────────────
def main():
    logger.info("🚀 Starting E-Commerce Event Producer")
    logger.info(f"📡 Kafka broker : {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"📤 Topic        : {KAFKA_TOPIC}")
    logger.info(f"⏱  Sleep interval: {SLEEP_INTERVAL}s")

    producer     = create_kafka_producer()
    events_sent  = 0
    events_failed = 0

    try:
        while running:
            event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]
            event      = generate_event(event_type)

            # validate before sending — catch bad data early
            if not validate_event(event):
                events_failed += 1
                continue

            # send with delivery callbacks
            producer.send(
                KAFKA_TOPIC,
                value=event
            ).add_callback(on_send_success).add_errback(
                on_send_error(producer, event)
            )

            events_sent += 1

            logger.info(
                f"Event #{events_sent:<6} | "
                f"type={event['event_type']:<22} | "
                f"product={event['product_name']:<25} | "
                f"user={event['user_id']}"
            )

            # flush every N events
            if events_sent % FLUSH_INTERVAL == 0:
                producer.flush()
                logger.info(
                    f"--- Flushed | sent={events_sent} "
                    f"failed={events_failed} ---"
                )

            time.sleep(SLEEP_INTERVAL)

    finally:
        logger.info("🔄 Flushing remaining messages...")
        producer.flush()
        producer.close()
        logger.info(
            f"✅ Producer shut down cleanly | "
            f"sent={events_sent} failed={events_failed}"
        )


if __name__ == '__main__':
    main()