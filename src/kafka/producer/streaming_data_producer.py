import json
import logging
import os
import random
import time 
from datetime import datetime
import pytz

import pandas as pd
from confluent_kafka import Producer
from dotenv import load_dotenv
from typing import Optional
from alpaca.data import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

#Load Env Variables
load_dotenv()

#Configure Logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

logger = logging.getLogger(__name__)

#Kafka Variables
KAFKA_BOOTSTRAP_SERVER=os.getenv('KAFKA_BOOTSTRAP_SERVER')
KAFKA_TOPIC_REALTIME=os.getenv('KAFKA_TOPIC_REALTIME')

#Define stocks to collect for realtime data
STOCKS = [
    "AAPL",
    "MSFT",
    "GOOGL",
    "AMZN",
    "META",
    "TSLA",
    "NVDA",
    "INTC",
    "JPM",
    "V"
]

class StreamDataCollector:
    def __init__(self, bootstrap_servers=KAFKA_BOOTSTRAP_SERVER, topic=KAFKA_TOPIC_REALTIME, interval=30):

        self.logger = logger
        self.topic = topic
        self.interval = interval

        self.current_stocks = STOCKS.copy()

        self.producer_conf = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "continuous-stock-data-producer"
        }

        try:
            self.producer = Producer(self.producer_conf)
            self.logger.info(f"Producer initialized. Sending to: {bootstrap_servers}, Topic: {topic}")
        except Exception as e:
            self.logger.error(f"Failed to create Kafka Producer: {e}")
            raise

    def delivery_report(self, err, msg):
        if err is not None:
            self.logger.error(f"Delivery failed for message: {msg}")
        else:
            self.logger.info(f"Message delivered successfully to topic {msg.topic} [{msg.partition()}]")

    def fetch_realtime_data(self, symbol:str):
        try:
            self.logger.info(f"Fetching realtime data for {symbol}")

            API_KEY = os.getenv('ALPACA_API_KEY')
            SECRET_KEY = os.getenv('ALPACA_SECRET_KEY')

            client = StockHistoricalDataClient(API_KEY, SECRET_KEY)

            market_tz = pytz.timezone("America/New_York")

            start = market_tz.localize(datetime(2025, 12, 8, 9, 30)) # Open
            end   = market_tz.localize(datetime(2025, 12, 12, 16, 0)) # End

            request_params = StockBarsRequest(
                    symbol_or_symbols=symbol,
                    timeframe=TimeFrame.Minute,
                    start=start,
                    end=end
                )

            bars = client.get_stock_bars(request_params)

            df = bars.df

            df = df.reset_index()

            df["timestamp"] = [row.to_pydatetime().isoformat() for row in df['timestamp']]

            self.logger.info(f"Successfully fetched {len(df)} days of data for {symbol}")

            return df

        except Exception as e:
            self.logger.error(f"Failed to fetch realtime data for {symbol} {e}")
            return None
        
    def producer_to_kafka(self, df: pd.DataFrame, symbol = str):
        
        batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
        df["batch_id"] = batch_id
        df["batch_date"] = datetime.now().strftime("%Y-%m-%d")

        records = df.to_dict(orient="records")

        successful_records = 0
        failed_records = 0

        for record in records:

            try:
                data = json.dumps(record)

                self.producer.produce(
                    topic=self.topic,
                    key=symbol,
                    value=data,
                    callback=self.delivery_report
                )

                self.producer.poll(0)

                successful_records += 1

            except Exception as e:
                self.logger.error(f"Failed to produce message for {symbol} {e}")
                failed_records += 1

            self.producer.flush()
            self.logger.info(f"Successfully produced {successful_records} records for {symbol} and failed {failed_records}")
        
    def collect_realtime_data(self):
        symbols = STOCKS

        self.logger.info(f"Starting realtime data collection for {len(symbols)} symbols")

        successul_symbols = 0
        failed_symbols = 0

        for symbol in symbols:
            try:
                #Fetch realtime data
                df = self.fetch_realtime_data(symbol)

                print(df.head(5))

                if df is not None and not df.empty:
                    self.producer_to_kafka(df, symbol)
                    successul_symbols += 1
                else:
                    self.logger.warning(f"No data returned for {symbol}")
                    failed_symbols += 1
            except Exception as e:
                self.logger.error(f"Error processing {symbol}: {e}")
                failed_symbols += 1

            time.sleep(1)

        self.logger.info(f"Realtime data collection completed. Successful: {successul_symbols}, Failed: {failed_symbols}")
    
def main():
    """Main function to run the Kafka Producer"""
    try:
        logger.info(f"Starting Streaming Stock Data Collector")

        producer = StreamDataCollector(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
            topic=KAFKA_TOPIC_REALTIME,
            interval=5 # Generate data every 30 seconds
        )

        # start producing stock data
        producer.collect_realtime_data()

    except Exception as e:
        logger.error(f"Fatal error: {e}")

if __name__ == "__main__":
    main()