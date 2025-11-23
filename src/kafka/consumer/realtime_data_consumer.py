import json 
import logging
import os
import tempfile
import time

from io import StringIO, BytesIO
from datetime import datetime

import pandas as pd
import numpy as np 

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

KAFKA_BOOTSTRAP_SERVERS=os.getenv('KAFKA_BOOTSTRAP_SERVER')
KAFKA_TOPIC_REALTIME=os.getenv('KAFKA_TOPIC_REALTIME')
KAFKA_GROUP_ID=os.getenv('KAFKA_GROUP_REALTIME_ID')
DEFAULT_BATCH_SIZE=100
flush_time=time.time()
flush_interval=60 #seconds
messages = []

#MinIO configuration
MINIO_ACCESS_KEY=os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY=os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET=os.getenv("MINIO_BUCKET")
MINIO_ENDPOINT=os.getenv("MINIO_ENDPOINT")

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
    consumer.subscribe([KAFKA_TOPIC_REALTIME])

    logger.info(f"Starting consumer topic {KAFKA_TOPIC_REALTIME}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue

            try:
                key = msg.key().decode("utf-8") if msg.key() else None
                value = json.loads(msg.value().decode("utf-8"))

                messages.append(value)

                if len(messages) % 10 == 0:
                    logger.info(f"Consumed {len(messages)} messages in current batch")

                current_time = time.time()

                if ((len(messages) >= DEFAULT_BATCH_SIZE) or (current_time - flush_time >= flush_interval and len(messages) > 0)):

                    df = pd.DataFrame(messages)

                    now = datetime.now()
                    timestamp = now.strftime('%H%M%S')

                    #Save to MinIO

                    object_name = f"raw/realtime/year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}/stock_data_{timestamp}.csv"

                    csv_buffer = StringIO()
                    df.to_csv(csv_buffer, index=False)
                    data_bytes = csv_buffer.getvalue().encode("utf-8")
                    data_stream = BytesIO(data_bytes)

                    try:

                        minio_client.put_object(
                            MINIO_BUCKET,
                            object_name=object_name,
                            data=data_stream,
                            length=len(data_bytes),
                            content_type="text/csv"
                        )

                        msg_count = len(messages)

                        logger.info(f"Wrote data for {msg_count} to s3://{MINIO_BUCKET}/{object_name}")
                    
                    except S3Error as e:
                        logger.error(f"Error writing to S3: {e}")
                        raise

                    messages.clear()
                    flush_time = time.time()

                    consumer.commit()

            except Exception as e:
                logger.error(f"Error processing message: {e}")

    except KeyboardInterrupt:
        logger.info("Stopping consumer")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()