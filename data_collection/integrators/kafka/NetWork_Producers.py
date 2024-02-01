import sys
import os
import json
import logging
import time
from datetime import datetime
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from datetime import datetime

sys.path.append("/home/hadoop/aviation-data-analytics/data_collection/scrappers/")

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

def produce_to_Topics(country , departures_topic, arrivals_topic, airports_topic, producer_logger):
    try:
        producer = Producer({"bootstrap.servers": "localhost:9092"})

        while True  :
            scrapper = Airports_Network_Scrapper('/home/hadoop/aviation-data-analytics/data_collection/collected_data/collected_airports_data.csv')

            for result in scrapper.get_Density(country):
                # Parse the JSON result
                data = json.loads(result)

                # Check if the 'departures' key is present and produce each departure as a separate message
                if 'departures' in data:
                    for departure in data["departures"]:
                        producer.produce(departures_topic, key="departure", value=json.dumps(departure))
                        time.sleep(1)
                        producer_logger.info(f"Departure Produced Successfully to {departures_topic}")

                # Check if the 'arrivals' key is present and produce each arrival as a separate message
                if 'arrivals' in data:
                    for arrival in data["arrivals"]:
                        producer.produce(arrivals_topic, key="arrival", value=json.dumps(arrival))
                        time.sleep(2)
                        producer_logger.info(f"Arrival Produced Successfully to {arrivals_topic}")

                # Check if the 'airport_info' key is present and produce airport information
                if 'airport_info' in data:
                    airport_info_message = json.dumps(data["airport_info"])
                    producer.produce(airports_topic, key="airport_info", value=airport_info_message)
                    time.sleep(3)
                    producer_logger.info(f"Airport Information Produced Successfully to {airports_topic}")

                # Flush only if everything is successful
                producer.flush()

            time.sleep(5)

    except StopIteration:
        producer_logger.info("Data generator exhausted. Stopping Kafka Producer.")

    except Exception as e:
        error_message = "Error producing to Kafka: " + str(e)
        producer_logger.error(error_message)

def runKafkaProducer(country ,departures_topic, arrivals_topic, airports_topic):
    producer_logger = setup_producer_logging()

    try:
        producer_logger.info("Kafka Producer started.")

        # Create a Kafka admin client
        admin_client = AdminClient({"bootstrap.servers": "localhost:9092"})

        # Check if the topics exist, and create them if not
        for topic in [departures_topic, arrivals_topic, airports_topic]:
            existing_topics = admin_client.list_topics().topics
            if topic not in existing_topics:
                create_kafka_topic(topic, admin_client, producer_logger)

        # Start producing to all topics simultaneously
        produce_to_Topics(country ,departures_topic, arrivals_topic, airports_topic, producer_logger)

    except KeyboardInterrupt:
        producer_logger.info("Kafka Producer Stopped")

    except Exception as e:
        error_message = "An unexpected error occurred in Kafka Producer: " + str(e)
        producer_logger.error(error_message)

# Example Usage
if __name__ == "__main__":
    country = "Morocco" 
    departures_topic = "departures_topic"
    arrivals_topic = "arrivals_topic"
    airports_topic = "airports_topic"

    runKafkaProducer(country, departures_topic, arrivals_topic, airports_topic)
    