"""
producer/kafka_producer.py
===========================
Kafka Producer for the Logistics Data Pipeline.

WHAT THIS SCRIPT DOES:
    1. Reads delivery trip truck data from a CSV file using Pandas.
    2. Connects to Confluent Schema Registry to fetch the Avro schema.
    3. Serializes each row into Avro format.
    4. Publishes each record to the 'delivery_trip_truck' Kafka topic.

ARCHITECTURAL DECISIONS:
    - We use the BookingID as the Kafka message key. This ensures that all GPS pings
      for the same booking land on the same partition, preserving ordering per trip.
      TRADE-OFF: This could cause partition skew if some bookings have many more pings
      than others. For our ~6,880 record dataset, this is not a concern. In a production
      system with millions of records, you might use a hash of BookingID + timestamp.

    - We flush after every message with a small delay (0.1s). This is intentional for
      the showreel demo to make the streaming visible in real-time. In production, you
      would batch messages and flush periodically for much higher throughput.

    - NaN/null handling: Pandas reads missing CSV values as NaN (float). Avro requires
      explicit nulls for union types ["null", "string"]. We convert all NaN and 'NULL'
      string values to Python None before serialization.

USAGE:
    python -m producer.kafka_producer --csv-path data/delivery_trip_truck_data.csv
    python -m producer.kafka_producer --csv-path data/delivery_trip_truck_data.csv --delay 0.5
"""

import sys
import os
import time
import argparse
import logging
import math

import pandas as pd
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from dotenv import load_dotenv

# Add project root to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config.config import KAFKA_CONFIG, KAFKA_TOPIC, SCHEMA_REGISTRY_CONFIG, SCHEMA_SUBJECT

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('kafka_producer')


def delivery_report(err, msg):
    """
    Callback invoked once per message to indicate delivery result.

    WHY A CALLBACK?
        Kafka producers are asynchronous. When you call produce(), the message
        is placed in an internal buffer. The actual send happens in the background.
        This callback tells us whether each message was successfully delivered
        or if it failed (e.g., broker down, topic doesn't exist, auth error).
    """
    if err is not None:
        logger.error(f"DELIVERY FAILED for key [{msg.key()}]: {err}")
    else:
        logger.info(
            f"DELIVERED: key=[{msg.key()}] -> topic={msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )


def clean_value(value, field_name: str, schema_type: str):
    """
    Clean a single field value to match the Avro schema expectations.

    TRADE-OFF: We handle type conversion here rather than in a separate ETL step.
    This keeps the pipeline simple (fewer moving parts), but it means the producer
    has some transformation logic mixed with publishing logic. In a larger system,
    you might have a dedicated data cleaning step before the producer.
    """
    # Handle NaN, None, and null-like strings
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, str) and value.strip().upper() in ('NULL', 'NA', 'NONE', ''):
        return None

    # Type-specific conversions
    if schema_type == 'double':
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    elif schema_type == 'string':
        return str(value).strip()

    return value


# Schema field types lookup — derived from our Avro schema
# This maps each field name to its non-null type in the union
FIELD_TYPES = {
    'GpsProvider': 'string',
    'BookingID': 'string',
    'Market_Regular': 'string',
    'BookingID_Date': 'string',
    'vehicle_no': 'string',
    'Origin_Location': 'string',
    'Destination_Location': 'string',
    'Org_lat_lon': 'string',
    'Des_lat_lon': 'string',
    'Data_Ping_time': 'string',
    'Planned_ETA': 'string',
    'Current_Location': 'string',
    'DestinationLocation': 'string',
    'actual_eta': 'string',
    'Curr_lat': 'double',
    'Curr_lon': 'double',
    'ontime': 'string',
    'delay': 'string',
    'OriginLocation_Code': 'string',
    'DestinationLocation_Code': 'string',
    'trip_start_date': 'string',
    'trip_end_date': 'string',
    'TRANSPORTATION_DISTANCE_IN_KM': 'double',
    'vehicleType': 'string',
    'Minimum_kms_to_be_covered_in_a_day': 'string',
    'Driver_Name': 'string',
    'Driver_MobileNo': 'string',
    'customerID': 'string',
    'customerNameCode': 'string',
    'supplierID': 'string',
    'supplierNameCode': 'string',
    'Material_Shipped': 'string',
}


def create_producer() -> SerializingProducer:
    """
    Initialize and return a configured Kafka SerializingProducer.

    The producer uses:
    - StringSerializer for keys (BookingID as string)
    - AvroSerializer for values (entire record serialized per Avro schema)
    - Schema Registry client to fetch/register the Avro schema
    """
    logger.info("Connecting to Schema Registry...")
    schema_registry_client = SchemaRegistryClient(SCHEMA_REGISTRY_CONFIG)

    # Fetch the latest schema version from the registry
    logger.info(f"Fetching schema for subject: {SCHEMA_SUBJECT}")
    schema_str = schema_registry_client.get_latest_version(SCHEMA_SUBJECT).schema.schema_str
    logger.info("Schema fetched successfully from registry")

    # Create serializers
    key_serializer = StringSerializer('utf_8')
    avro_serializer = AvroSerializer(schema_registry_client, schema_str)

    # Build and return the producer
    producer = SerializingProducer({
        'bootstrap.servers': KAFKA_CONFIG['bootstrap.servers'],
        'security.protocol': KAFKA_CONFIG['security.protocol'],
        'sasl.mechanisms': KAFKA_CONFIG['sasl.mechanisms'],
        'sasl.username': KAFKA_CONFIG['sasl.username'],
        'sasl.password': KAFKA_CONFIG['sasl.password'],
        'key.serializer': key_serializer,
        'value.serializer': avro_serializer,
    })

    logger.info("Kafka producer created successfully")
    return producer


def load_and_clean_csv(csv_path: str) -> pd.DataFrame:
    """
    Load the CSV file and perform initial cleaning.

    NOTE ON THE CSV QUIRK:
        The column 'Market_Regular ' has a trailing space in the header.
        We strip all column names to handle this gracefully.
    """
    logger.info(f"Loading CSV from: {csv_path}")
    df = pd.read_csv(csv_path)

    # Strip whitespace from column names (handles 'Market_Regular ' -> 'Market_Regular')
    df.columns = df.columns.str.strip()

    logger.info(f"Loaded {len(df)} records with {len(df.columns)} columns")
    logger.info(f"Columns: {list(df.columns)}")

    return df


def produce_records(producer: SerializingProducer, df: pd.DataFrame, delay: float = 0.1):
    """
    Iterate through the DataFrame and publish each row to Kafka.

    Parameters
    ----------
    producer : SerializingProducer
        The configured Kafka producer instance.
    df : pd.DataFrame
        The cleaned logistics data.
    delay : float
        Seconds to wait between messages (for demo visibility). Set to 0 for max throughput.
    """
    total = len(df)
    success_count = 0
    error_count = 0

    logger.info(f"Starting to produce {total} records to topic '{KAFKA_TOPIC}'...")

    for index, row in df.iterrows():
        try:
            # Build the Avro-compatible value dictionary
            data_value = {}
            for field_name, field_type in FIELD_TYPES.items():
                raw_value = row.get(field_name, None)
                data_value[field_name] = clean_value(raw_value, field_name, field_type)

            # Use BookingID as the message key for partition affinity
            message_key = str(data_value.get('BookingID', index))

            # Produce to Kafka
            producer.produce(
                topic=KAFKA_TOPIC,
                key=message_key,
                value=data_value,
                on_delivery=delivery_report
            )

            # Flush to ensure delivery (for demo; in production, flush periodically)
            producer.flush()
            success_count += 1

            # Progress logging every 500 records
            if (index + 1) % 500 == 0:
                logger.info(f"Progress: {index + 1}/{total} records produced")

            # Configurable delay for demo purposes
            if delay > 0:
                time.sleep(delay)

        except Exception as e:
            error_count += 1
            logger.error(f"Failed to produce record at index {index}: {e}")

    logger.info(f"Production complete: {success_count} succeeded, {error_count} failed out of {total}")


def main():
    parser = argparse.ArgumentParser(description='Kafka Producer for Logistics Data')
    parser.add_argument(
        '--csv-path',
        type=str,
        default='data/delivery_trip_truck_data.csv',
        help='Path to the CSV file containing delivery trip data'
    )
    parser.add_argument(
        '--delay',
        type=float,
        default=0.1,
        help='Delay between messages in seconds (0 for max throughput)'
    )
    args = parser.parse_args()

    # Validate CSV path
    if not os.path.exists(args.csv_path):
        logger.error(f"CSV file not found: {args.csv_path}")
        sys.exit(1)

    # Create producer and load data
    producer = create_producer()
    df = load_and_clean_csv(args.csv_path)

    # Produce all records
    produce_records(producer, df, delay=args.delay)

    logger.info("All data successfully published to Kafka!")


if __name__ == '__main__':
    main()
