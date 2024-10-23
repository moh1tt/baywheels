# Consumer Script: consume_station_status.py
import requests
import logging
import json
from datetime import datetime
from confluent_kafka import Consumer, KafkaException
from influxdb_client import InfluxDBClient, Point, WriteOptions

# Set up logging configuration
logging.basicConfig(level=logging.INFO)

# InfluxDB configuration
INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = "YOcj6Q-skIUtAlseN1g1yQG2C0NrNwweqeOszrEbi16mucRJjYsxnQEJzYuT-I7h8LmFzsexuv5kyulByVMCfg=="
INFLUXDB_ORG = "mohit"  # Replace with your actual organization
INFLUXDB_BUCKET = "baywheels"

influx_client = InfluxDBClient(
    url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = influx_client.write_api(
    write_options=WriteOptions(batch_size=5000))


def consume_station_status(topic='station_status'):
    # Kafka Consumer Configuration
    conf = {
        'bootstrap.servers': 'localhost:29092',
        'group.id': 'station_status_consumer_group',
        'auto.offset.reset': 'earliest',
    }

    consumer = Consumer(conf)
    consumer.subscribe([topic])

    try:
        while True:
            message = consumer.poll(timeout=1.0)

            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())

            station_status = json.loads(message.value().decode('utf-8'))
            # Write to InfluxDB
            for station in station_status['data']['stations']:
                point = Point("station_status")\
                    .tag("station_id", station['station_id'])\
                    .field("num_bikes_available", station['num_bikes_available'])\
                    .field("num_docks_available", station['num_docks_available'])\
                    .time(datetime.utcnow())  # Use current UTC time
                write_api.write(bucket=INFLUXDB_BUCKET, record=point)
                logging.info(
                    "Written data to InfluxDB for station_id: %s", station['station_id'])

    except KeyboardInterrupt:
        logging.info("Consumer interrupted.")
    except KafkaException as e:
        logging.error("Kafka error: %s", e)
    finally:
        consumer.close()
        influx_client.close()
        logging.info("Consumer and InfluxDB client closed.")


if __name__ == "__main__":
    consume_station_status()
