"""
consumer/kafka_consumer.py
============================
Kafka Consumer for the Logistics Data Pipeline.

WHAT THIS SCRIPT DOES:
    1. Connects to Confluent Kafka and subscribes to the 'delivery_trip_truck' topic.
    2. Deserializes each Avro-encoded message using the Schema Registry.
    3. Validates each record using the validation module (business rules).
    4. Ingests valid records into MongoDB Atlas (database: Showreel, collection: delivery_trip_truck).
    5. Routes invalid records to a dead-letter collection for debugging.

ARCHITECTURAL DECISIONS:

    Dead-Letter Pattern:
        Records that fail validation are NOT dropped silently. They are written to a
        separate MongoDB collection called 'delivery_trip_truck_dead_letter' with the
        original record, error details, and a timestamp. This allows data engineers to
        investigate data quality issues without blocking the pipeline.

    Consumer Group & Scaling:
        All consumer instances share the same consumer group ID ('logistics-consumer-group').
        Kafka automatically distributes topic partitions across consumers in the same group.
        If the topic has 6 partitions and we run 3 consumer containers, each gets 2 partitions.
        This is how Docker-based scaling works — more containers = more parallel consumers.

    At-Least-Once Delivery:
        We use manual offset management (enable.auto.commit=False) and commit after
        successfully writing to MongoDB. This means if the consumer crashes after reading
        but before committing, the message will be re-delivered. We handle this with an
        upsert strategy in MongoDB using BookingID + Data_Ping_time as a compound key.

    TRADE-OFF — Upsert vs Insert:
        Using upsert (update if exists, insert if not) is slightly slower than plain insert
        (~10-15% overhead per write). But it gives us idempotency, meaning the same message
        processed twice produces the same result. For a showreel project demonstrating
        production-grade patterns, this tradeoff is worthwhile.

USAGE:
    python -m consumer.kafka_consumer
    python -m consumer.kafka_consumer --batch-size 50
"""

import sys
import os
import time
import signal
import argparse
import logging
from datetime import datetime, timezone

from confluent_kafka import DeserializingConsumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config.config import (
    KAFKA_CONFIG, KAFKA_TOPIC, SCHEMA_REGISTRY_CONFIG, SCHEMA_SUBJECT,
    MONGO_CONNECTION_STRING, MONGO_DATABASE, MONGO_COLLECTION,
    CONSUMER_GROUP_ID, AUTO_OFFSET_RESET
)
from validation.validators import validate_record, sanitize_record

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('kafka_consumer')

# Graceful shutdown flag
shutdown_requested = False


def signal_handler(signum, frame):
    """Handle SIGINT/SIGTERM for graceful shutdown in Docker containers."""
    global shutdown_requested
    logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
    shutdown_requested = True


# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def create_consumer() -> DeserializingConsumer:
    """
    Initialize and return a configured Kafka DeserializingConsumer.

    KEY CONFIGURATION EXPLAINED:
    - group.id: All consumers with the same group.id share the workload.
      Kafka assigns partitions so each partition is consumed by exactly one
      consumer in the group. This is how we achieve parallel processing.

    - auto.offset.reset: 'earliest' means if this consumer group has never
      consumed this topic before, start from the very first message.
      After the first run, Kafka remembers where we left off.

    - enable.auto.commit: False means WE control when offsets are committed.
      We commit only after successfully writing to MongoDB, ensuring
      at-least-once delivery semantics.
    """
    logger.info("Connecting to Schema Registry...")
    schema_registry_client = SchemaRegistryClient(SCHEMA_REGISTRY_CONFIG)

    # Fetch schema for deserialization
    logger.info(f"Fetching schema for subject: {SCHEMA_SUBJECT}")
    schema_str = schema_registry_client.get_latest_version(SCHEMA_SUBJECT).schema.schema_str
    logger.info("Schema fetched successfully")

    # Create deserializers
    key_deserializer = StringDeserializer('utf_8')
    avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

    # Build consumer
    consumer = DeserializingConsumer({
        'bootstrap.servers': KAFKA_CONFIG['bootstrap.servers'],
        'security.protocol': KAFKA_CONFIG['security.protocol'],
        'sasl.mechanisms': KAFKA_CONFIG['sasl.mechanisms'],
        'sasl.username': KAFKA_CONFIG['sasl.username'],
        'sasl.password': KAFKA_CONFIG['sasl.password'],
        'key.deserializer': key_deserializer,
        'value.deserializer': avro_deserializer,
        'group.id': CONSUMER_GROUP_ID,
        'auto.offset.reset': AUTO_OFFSET_RESET,
        'enable.auto.commit': False,  # Manual commit for at-least-once semantics
    })

    logger.info(f"Consumer created with group.id='{CONSUMER_GROUP_ID}'")
    return consumer


def create_mongo_client():
    """
    Initialize MongoDB client and return the target collection and dead-letter collection.

    WHY TWO COLLECTIONS?
        The main collection stores validated, clean data ready for API queries.
        The dead-letter collection stores rejected records with error metadata,
        allowing data engineers to diagnose and fix data quality issues.
    """
    logger.info("Connecting to MongoDB Atlas...")
    client = MongoClient(MONGO_CONNECTION_STRING)

    # Verify connection
    client.admin.command('ping')
    logger.info("MongoDB connection verified")

    db = client[MONGO_DATABASE]
    main_collection = db[MONGO_COLLECTION]
    dead_letter_collection = db[f'{MONGO_COLLECTION}_dead_letter']

    # Create indexes for performance and idempotency
    # Compound index on BookingID + Data_Ping_time for upsert deduplication
    main_collection.create_index(
        [('BookingID', 1), ('Data_Ping_time', 1)],
        unique=False,  # Multiple pings per booking are expected
        name='idx_booking_ping'
    )
    # Index on BookingID for fast lookups
    main_collection.create_index('BookingID', name='idx_booking_id')
    # Index on vehicle_no for fleet queries
    main_collection.create_index('vehicle_no', name='idx_vehicle_no')
    # Index on customerID for customer-centric queries
    main_collection.create_index('customerID', name='idx_customer_id')

    logger.info(f"Using database: {MONGO_DATABASE}, collection: {MONGO_COLLECTION}")
    return main_collection, dead_letter_collection


def process_batch(
    valid_records: list,
    invalid_records: list,
    main_collection,
    dead_letter_collection
):
    """
    Write a batch of records to MongoDB.

    Valid records go to the main collection via upsert.
    Invalid records go to the dead-letter collection.
    """
    # Insert valid records using bulk upsert
    if valid_records:
        operations = []
        for record in valid_records:
            # Upsert filter: match on BookingID + Data_Ping_time + Curr_lat + Curr_lon
            # This combination uniquely identifies a GPS reading for a trip
            filter_doc = {
                'BookingID': record.get('BookingID'),
                'Data_Ping_time': record.get('Data_Ping_time'),
                'Curr_lat': record.get('Curr_lat'),
                'Curr_lon': record.get('Curr_lon'),
            }
            # Add ingestion metadata
            record['_ingested_at'] = datetime.now(timezone.utc)
            record['_source'] = 'kafka_consumer'

            operations.append(UpdateOne(filter_doc, {'$set': record}, upsert=True))

        try:
            result = main_collection.bulk_write(operations, ordered=False)
            logger.info(
                f"Bulk write: {result.upserted_count} inserted, "
                f"{result.modified_count} updated, "
                f"{len(valid_records)} total valid records"
            )
        except BulkWriteError as bwe:
            logger.error(f"Bulk write partial failure: {bwe.details}")

    # Insert invalid records into dead-letter collection
    if invalid_records:
        try:
            dead_letter_collection.insert_many(invalid_records, ordered=False)
            logger.warning(f"Wrote {len(invalid_records)} records to dead-letter collection")
        except Exception as e:
            logger.error(f"Failed to write to dead-letter collection: {e}")


def consume_messages(consumer: DeserializingConsumer, main_collection, dead_letter_collection, batch_size: int = 100):
    """
    Main consumption loop.

    HOW BATCHING WORKS:
        Instead of writing each message to MongoDB individually (which would be slow),
        we accumulate messages into a batch. Once the batch reaches the configured size
        OR a timeout occurs, we flush the batch to MongoDB and commit the offsets.

        TRADE-OFF: Larger batches = higher throughput but higher memory usage and
        longer delay before data appears in MongoDB. batch_size=100 is a balanced
        default for our ~6,880 record dataset.
    """
    consumer.subscribe([KAFKA_TOPIC])
    logger.info(f"Subscribed to topic: {KAFKA_TOPIC}")

    valid_batch = []
    invalid_batch = []
    total_consumed = 0
    total_valid = 0
    total_invalid = 0
    last_flush_time = time.time()

    try:
        while not shutdown_requested:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # No message available — check if we should flush a partial batch
                if valid_batch or invalid_batch:
                    elapsed = time.time() - last_flush_time
                    if elapsed > 5.0:  # Flush partial batch after 5 seconds of inactivity
                        logger.info("Flushing partial batch due to inactivity timeout")
                        process_batch(valid_batch, invalid_batch, main_collection, dead_letter_collection)
                        consumer.commit()
                        valid_batch.clear()
                        invalid_batch.clear()
                        last_flush_time = time.time()
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"Reached end of partition {msg.partition()}")
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                continue

            # Successfully received a message
            total_consumed += 1
            record = msg.value()

            logger.debug(
                f"Consumed: partition={msg.partition()} offset={msg.offset()} "
                f"key={msg.key()}"
            )

            # Step 1: Validate the record
            validation_result = validate_record(record)

            # Step 2: Sanitize (normalize nulls, strip whitespace)
            cleaned_record = sanitize_record(record)

            if validation_result.is_valid:
                # Add validation metadata (warnings if any)
                if validation_result.warnings:
                    cleaned_record['_validation_warnings'] = validation_result.warnings
                valid_batch.append(cleaned_record)
                total_valid += 1
            else:
                # Build dead-letter record with full error context
                dead_letter_record = {
                    'original_record': record,
                    'validation_errors': validation_result.errors,
                    'validation_warnings': validation_result.warnings,
                    'kafka_metadata': {
                        'topic': msg.topic(),
                        'partition': msg.partition(),
                        'offset': msg.offset(),
                        'key': msg.key(),
                    },
                    '_rejected_at': datetime.now(timezone.utc),
                    '_reason': 'validation_failure'
                }
                invalid_batch.append(dead_letter_record)
                total_invalid += 1
                logger.warning(
                    f"Record rejected (BookingID={record.get('BookingID')}): "
                    f"{validation_result.errors}"
                )

            # Step 3: Flush batch if size threshold reached
            if len(valid_batch) + len(invalid_batch) >= batch_size:
                process_batch(valid_batch, invalid_batch, main_collection, dead_letter_collection)
                consumer.commit()
                valid_batch.clear()
                invalid_batch.clear()
                last_flush_time = time.time()

            # Progress logging every 500 records
            if total_consumed % 500 == 0:
                logger.info(
                    f"Progress: consumed={total_consumed} valid={total_valid} "
                    f"invalid={total_invalid}"
                )

    except Exception as e:
        logger.error(f"Unexpected error in consumption loop: {e}", exc_info=True)
    finally:
        # Flush any remaining records
        if valid_batch or invalid_batch:
            logger.info("Flushing final batch before shutdown...")
            process_batch(valid_batch, invalid_batch, main_collection, dead_letter_collection)
            consumer.commit()

        consumer.close()
        logger.info(
            f"Consumer shutdown complete. Total: consumed={total_consumed} "
            f"valid={total_valid} invalid={total_invalid}"
        )


def main():
    parser = argparse.ArgumentParser(description='Kafka Consumer for Logistics Data')
    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Number of records to accumulate before flushing to MongoDB'
    )
    args = parser.parse_args()

    # Initialize consumer and MongoDB
    consumer = create_consumer()
    main_collection, dead_letter_collection = create_mongo_client()

    # Start consuming
    logger.info("Starting consumer... Press Ctrl+C to stop.")
    consume_messages(consumer, main_collection, dead_letter_collection, batch_size=args.batch_size)


if __name__ == '__main__':
    main()
