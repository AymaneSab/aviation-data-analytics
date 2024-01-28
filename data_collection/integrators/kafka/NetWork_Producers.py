import sys
import os
import json
import logging
import time
from datetime import datetime
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from datetime import datetime
from RealTime_AirportNetwork_Scrapper import Airports_Network_Scrapper 

# Set up Loggin Function
def setup_producer_logging():

    log_directory = "Log/Producer_Log_Files"
    os.makedirs(log_directory, exist_ok=True)

    log_filename = datetime.now().strftime("%Y-%m-%d_%H-%M-%S.log")
    log_filepath = os.path.join(log_directory, log_filename)

    logging.basicConfig(filename=log_filepath, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    producer_logger = logging.getLogger(__name__)  

    return producer_logger

def create_kafka_topic(topic, admin_client, producer_logger):
    try:
        topic_spec = NewTopic(topic, num_partitions=1, replication_factor=1)

        admin_client.create_topics([topic_spec])

        separator = '-' * 30
        producer_logger.info(f"{topic} {separator} Created Successfully: ")

    except Exception as e:
        error_message = "Error creating Kafka topic: " + str(e)
        producer_logger.error(error_message)

def produce_to_Topics(departures_topic, arrivals_topic, producer_logger):
    try:
        producer = Producer({"bootstrap.servers": "localhost:9092"})

        scrapper = Airports_Network_Scrapper('/home/hadoop/aviation-data-analytics/data_collection/collected_data/collected_airports_data.csv')

        for result in scrapper.get_Density('Morocco'):
            # Parse the JSON result
            data = json.loads(result)

            # Check if the 'departures' key is present and produce to the departures topic
            if 'departures' in data:
                producer.produce(departures_topic, key="departure", value=json.dumps(data["departures"]))
                producer_logger.info(f"Departures Produced Successfully to {departures_topic}")

            # Check if the 'arrivals' key is present and produce to the arrivals topic
            if 'arrivals' in data:
                producer.produce(arrivals_topic, key="arrival", value=json.dumps(data["arrivals"]))
                producer_logger.info(f"Arrivals Produced Successfully to {arrivals_topic}")

            # Flush only if everything is successful
            producer.flush()

    except StopIteration:
        producer_logger.info("Data generator exhausted. Stopping Kafka Producer.")

    except Exception as e:
        error_message = "Error producing to Kafka: " + str(e)
        producer_logger.error(error_message)

# Example usage:
departures_topic = "Departures"
arrivals_topic = "Arrivals"


