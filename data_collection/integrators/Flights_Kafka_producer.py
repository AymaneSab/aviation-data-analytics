import sys
import os
import json
import logging
import time
from datetime import datetime 
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from datetime import datetime

# Set up Loggin Function
def setup_producer_logging():

    log_directory = "Log/Flights_Producer"
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

        producer_logger.info(f"{topic} Created Successfully: ")

    except Exception as e:
        error_message = "Error creating Kafka topic: " + str(e)
        producer_logger.error(error_message)

def produce_to_Topics(Topic,data,key, producer_logger):
    try:
        producer = Producer({"bootstrap.servers": "localhost:9092"})  # Kafka broker address

        while True:
            # Fetch the next movie data

            data = json.loads(data)

            # Check if "release_date" is present and not null in the movie object
            try:
                # Produce to the movies topic
                producer.produce(Topic, key=key, value=json.dumps(data))
                producer_logger.info(f"Movie Produced Successfully to {Topic}: ")
                # Flush only if everything is successful
                producer.flush()

            except Exception as ex:
                # Log other validation errors
                error_message = f"Error validating Kafka message: {ex}"
                producer_logger.error(error_message)

    except StopIteration:
        producer_logger.info(" generator exhausted. Stopping Kafka Producer.")

    except Exception as e:
        error_message = "Error producing to Kafka: " + str(e)
        producer_logger.error(error_message)
