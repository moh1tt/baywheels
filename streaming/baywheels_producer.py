# Producer Script: fetch_and_produce_station_status.py
import requests
import logging
import json  # Import the json module
from confluent_kafka import Producer, KafkaError

# Set up logging configuration
logging.basicConfig(level=logging.INFO)


def delivery_report(err, msg):
    """Delivery report callback called on successful or failed delivery of message."""
    if err is not None:
        logging.error("Message delivery failed: %s", err)
    else:
        logging.info("Message delivered to %s [%d] at offset %d", msg.topic(
        ), msg.partition(), msg.offset())


def fetch_and_produce_station_status(topic='station_status'):
    # Kafka Producer Configuration
    producer = Producer(
        {'bootstrap.servers': 'localhost:29092'})  # Use port 29092

    try:
        # Fetch GBFS station status feed
        response = requests.get(
            "https://gbfs.lyft.com/gbfs/2.3/bay/en/station_status.json")
        response.raise_for_status()  # Raise an error for bad responses (4xx and 5xx)
        station_status = response.json()
        logging.info("Fetched station status successfully.")

        # Produce messages to Kafka topic
        producer.produce(topic, value=json.dumps(
            station_status), callback=delivery_report)
        producer.flush()  # Ensure all messages are delivered
        logging.info("Message sent to Kafka topic '%s'.", topic)

    except requests.exceptions.RequestException as e:
        logging.error("Failed to fetch station status: %s", e)
    except KafkaError as e:
        logging.error("Failed to produce message: %s", e)


if __name__ == "__main__":
    fetch_and_produce_station_status()
