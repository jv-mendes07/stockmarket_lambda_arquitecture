import json 
import logging
import os
import tempfile

from datetime import datetime

import pandas as pd
import numpy as np 

from fastavro import writer, parse_schema
from confluent_kafka import Consumer
from minio import Minio
from minio.error import S3Error

from dotenv import load_dotenv

#Load Env Variables
load_dotenv()

#Configure Logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS=os.getenv("KAFKA_BOOTSTRAP_SERVER_AIRFLOW")
KAFKA_TOPIC_BATCH=os.getenv("KAFKA_TOPIC_BATCH")
KAFKA_GROUP_ID=os.getenv("KAFKA_GROUP_BATCH_ID")

#MinIO configuration
MINIO_ACCESS_KEY=os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY=os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET=os.getenv("MINIO_BUCKET")
MINIO_ENDPOINT=os.getenv("MINIO_ENDPOINT")

avro_schema = {
    "type": "record",
    "name": "StockRecord",
    "fields": [
        {"name": "symbol", "type": "string"},
        {"name": "batch_date", "type": "string"},
        {"name": "open", "type": ["null", "double"], "default": None},
        {"name": "high", "type": ["null", "double"], "default": None},
        {"name": "low", "type": ["null", "double"], "default": None},
        {"name": "close", "type": ["null", "double"], "default": None},
        {"name": "volume", "type": ["null", "long"], "default": None}
    ]
}

def create_minio_client():
    """Initialize MinIO Client"""

    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def ensure_bucket_exists(minio_client, minio_bucket):
    """Ensure that the bucket exists on the MinIO"""
    try:
        if not minio_client.bucket_exists(minio_bucket):
            minio_client.make_bucket(minio_bucket)
            logger.info(f"Created bucket {minio_bucket}")
        else:
            logger.info(f"Bucket {minio_bucket} already exists")
    except S3Error as e:
        logger.error(f"Error creating bucket {minio_bucket}: {e}")
        raise

def main():
    # Create a MinIO client
    minio_client = create_minio_client()
    ensure_bucket_exists(minio_client, MINIO_BUCKET)

    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }

    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC_BATCH])

    logger.info(f"Starting consumer topic {KAFKA_TOPIC_BATCH}")

    max_empty_polls = 10

    empty_polls = 0

    parsed_schema = parse_schema(avro_schema)

    try:
        while True:
            
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                empty_polls += 1
                logger.info(f"Without message. Try {empty_polls}-{max_empty_polls}")

                if empty_polls >= max_empty_polls:
                    logger.info("Without new messages. Stopping the Consumer")
                    break

                continue

            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue

            try:

                empty_polls = 0

                data = json.loads(msg.value().decode("utf-8"))
                symbol = data['symbol']
                date = data['date']

                year, month, day = date.split('-')

                # Temp file path for AVRO
                temp_dir = tempfile.gettempdir()
                avro_file = os.path.join(temp_dir, f"{symbol}.avro")

                # Write AVRO file
                with open(avro_file, "wb") as out:
                    writer(out, parsed_schema, [data])

                #Save to MinIO

                object_name = f"raw/historical/year={year}/month={month}/day={day}/{symbol}_{datetime.now().strftime('%H%M%S')}.avro"

                minio_client.fput_object(
                    MINIO_BUCKET,
                    object_name=object_name,
                    file_path=avro_file
                )

                logger.info(f"Wrote data for {symbol} to s3://{MINIO_BUCKET}/{object_name}")
                
                os.remove(avro_file)

                consumer.commit()

            except Exception as e:
                logger.error(f"Error processing message: {e}")
    except KeyboardInterrupt:
        logger.info("Stopping consumer")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()