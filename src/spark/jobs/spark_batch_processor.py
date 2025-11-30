import os
import sys
import traceback
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

from dotenv import load_dotenv
from minio import Minio
import logging

load_dotenv()

#Configure Logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

logger = logging.getLogger(__name__)

#MinIO configuration
MINIO_ACCESS_KEY=os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY=os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET=os.getenv("MINIO_BUCKET")
MINIO_ENDPOINT_SPARK=os.getenv("MINIO_ENDPOINT")

def create_spark_session():
    logger.info("Initializing Spark Session with S3 Configuration")

    spark = (SparkSession.builder
             .appName("StockMarketBatchProcessor")
             .config(
                "spark.jars",
                "jars/hadoop-aws-3.3.2.jar,"
                "jars/aws-java-sdk-bundle-1.12.262.jar,"
                "jars/spark-avro_2.12-3.4.2.jar"
                )
             .getOrCreate())
    
    spark_conf = spark.sparkContext._jsc.hadoopConfiguration()
    spark_conf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    spark_conf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    spark_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT_SPARK)
    spark_conf.set("fs.s3a.path.style.access", "true")
    spark_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark_conf.set("fs.s3a.connection.ssl.enabled", "false")
    spark_conf.set("fs.s3a.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark Session initialized successfully")

    return spark

def read_data_from_s3(spark, date=None):
    logger.info("Reading data from S3")

    if date is None:
        process_date = datetime.now() - timedelta(days=0)
    else:
        process_date = datetime.strftime(date, "%Y-%m-%d")

    year = process_date.year
    month = process_date.month
    day = process_date.day

    s3_path = f"s3a://{MINIO_BUCKET}/raw/historical/year={year}/month={month:02d}/day={day:02d}/"

    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()

    # Import - Java Classes
    Path = sc._jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem

    # FileSystem MinIO/S3A
    fs = FileSystem.get(Path(s3_path).toUri(), hadoop_conf)

    files = []
    status_list = fs.listStatus(Path(s3_path))

    for status in status_list:
        name = status.getPath().toString()

        # .avro files
        if name.endswith(".avro") and status.isFile():
            files.append(name)

    logger.info(f"Files found: {files}")

    if not files:
        logger.info("No Avro files found.")
        return None

    logger.info(f"Reading data from: {s3_path}")

    try:

        df = spark.read.format("avro").load(files)

        print("Sample data:")
        df.show(5, truncate=False)
        df.printSchema()
        return df
    except Exception as e:
        logger.info(f"Error reading data from S3: {str(e)}")
        return None

def process_stock_data(df):
    logger.info(f"\n----- Processing Historical Stock Data")

    if df is None or df.count() == 0:
        logger.info("No data to process")
        return None
    try:
        record_count = df.count()
        logger.info(f"Record count: {record_count}")

        #Define window for daily metrics
        window_day = Window.partitionBy("symbol", "date")

        #calculate metrics
        df = df.withColumn("daily_open", F.first("open").over(window_day))
        df = df.withColumn("daily_high", F.max("high").over(window_day))
        df = df.withColumn("daily_low", F.min("low").over(window_day))
        df = df.withColumn("daily_volume", F.sum("volume").over(window_day))
        df = df.withColumn("daily_close", F.last("close").over(window_day))

        #calculate change from open
        df = df.withColumn("daily_change", (F.col("daily_close") - F.col("daily_open")) / F.col("daily_open") * 100)

        logger.info("Sample of processed data")
        df.select("symbol", "date", "daily_open", "daily_high", "daily_low", "daily_volume", "daily_close", 
                  "daily_change").show(5)
        
        return df
    
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        return None

def write_to_s3(df, date=None):
    logger.info("\n----------- Writing processed data to s3")

    if df is None:
        logger.info("No data to write")
        return None
    
    if date is None:
        processed_date = datetime.now().strftime("%Y-%m-%d")

    output_path = f"s3a://{MINIO_BUCKET}/processed/historical/date={processed_date}"
    logger.info(f"Writing processed data to: {output_path}")

    try:
        df.write.partitionBy("symbol").mode("overwrite").parquet(output_path)
        logger.info(f"Data written to s3: {output_path}")
    
    except Exception as e:
        logger.info(f"Error writing to s3: {str(e)}")
        return None

def main():
    """Main Function to process the historical data"""
    logger.info("\n=====================================================")
    logger.info("STARTING STOCK MARKET BATCH PROCESSOR")
    logger.info("=====================================================\n")

    date = None 

    spark = create_spark_session()

    try:

        df = read_data_from_s3(spark, date)

        if df is not None:
            processed_df = process_stock_data(df)

            if processed_df is not None:
                write_to_s3(processed_df)
                logger.info("Data written to S3")
            else:
                logger.info("Error processing data")
        
        else:
            logger.error("Failed to read data from S3")
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
    finally:
        logger.info("\nStopping Spark Session")
        spark.stop()
        logger.info("\n=====================================================")
        logger.info("BATCH PROCESSING COMPLETE")
        logger.info("=====================================================\n")

if __name__ == "__main__":
    main()