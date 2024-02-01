from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, ArrayType
from elasticsearch import Elasticsearch
from datetime import datetime
from elasticsearch.exceptions import RequestError
import os
import logging


def elastic_setup_logging():
    log_directory = "Log/ElasticSearch"

    os.makedirs(log_directory, exist_ok=True)

    log_filename = datetime.now().strftime("%Y-%m-%d_%H-%M-%S.log")
    log_filepath = os.path.join(log_directory, log_filename)

    logging.basicConfig(filename=log_filepath, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    consumer_logger = logging.getLogger(__name__)  
    
    return consumer_logger

def connectToelastic(elastic_logger):
    # We can directly do all Elasticsearch-related operations in our Spark script using this object.
    es = Elasticsearch("http://localhost:9200")

    if es:
        elastic_logger.info("Connected to elastic search Successfully")

        return es
    
def createAirportsIndex(esconnection, elastic_logger):
    my_index_body = {
        "settings": {
            "index": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            }
        },
        "mappings": {
            "properties": {
                "iata": {"type": "keyword"},
                "icao": {"type": "keyword"},
                "name": {"type": "keyword"},
                "location": {"type": "keyword"},  
                "latitude": {"type": "text"},
                "longitude": {"type": "text"}  ,
                "website": {"type": "text"}  ,
                "city": {"type": "keyword"}  ,
                "country": {"type": "keyword"}  ,
                "country_iso": {"type": "keyword"}  ,
                "phone": {"type": "text"}  
            }
        }
    }
    try:
        esconnection.indices.create(index="airports", body=my_index_body)
        elastic_logger.info("Index 'airports' created successfully.")
        
    except RequestError as e:
        if "resource_already_exists_exception" in str(e):
            elastic_logger.info("Index 'airports' already exists.")
        else:
            elastic_logger.error(f"Error creating index: {e}")

def createDeparturesIndex(esconnection, elastic_logger):
    my_index_body = {
        "settings": {
            "index": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            }
        },
        "mappings": {
            "properties": {
            "icao24": {"type": "text"},
            "first_seen": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
            "est_departure_airport": {"type": "keyword"},
            "last_seen": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
            "est_arrival_airport": {"type": "text"},  
            "callsign": {"type": "keyword"},
            "est_departure_airport_horiz_distance": {"type": "integer"},
            "est_departure_airport_vert_distance": {"type": "integer"},
            "est_arrival_airport_horiz_distance": {"type": "integer"},
            "est_arrival_airport_vert_distance": {"type": "integer"},
            "departure_airport_candidates_count": {"type": "integer"},
            "arrival_airport_candidates_count": {"type": "integer"},
            "date": {"type": "date", "format": "yyyy-MM-dd"},
            "airport_name": {"type": "keyword"},
            "airport_country": {"type": "keyword"},
            "airport_city": {"type": "keyword"}
            }
        }
    }
    try:
        esconnection.indices.create(index="departures", body=my_index_body)
        elastic_logger.info("Index 'departures' created successfully.")
    except RequestError as e:
        if "resource_already_exists_exception" in str(e):
            elastic_logger.info("Index 'departures' already exists.")
        else:
            elastic_logger.error(f"Error creating index: {e}")

def createArrivalsIndex(esconnection, elastic_logger):
    my_index_body = {
        "settings": {
            "index": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            }
        },
        "mappings": {
            "properties": {
            "icao24": {"type": "text"},
            "first_seen": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
            "est_departure_airport": {"type": "keyword"},
            "last_seen": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
            "est_arrival_airport": {"type": "text"},  
            "callsign": {"type": "keyword"},
            "est_departure_airport_horiz_distance": {"type": "integer"},
            "est_departure_airport_vert_distance": {"type": "integer"},
            "est_arrival_airport_horiz_distance": {"type": "integer"},
            "est_arrival_airport_vert_distance": {"type": "integer"},
            "departure_airport_candidates_count": {"type": "integer"},
            "arrival_airport_candidates_count": {"type": "integer"},
            "date": {"type": "date", "format": "yyyy-MM-dd"},
            "airport_name": {"type": "keyword"},
            "airport_country": {"type": "keyword"},
            "airport_city": {"type": "keyword"}
            }
        }
    }

    try:
        esconnection.indices.create(index="arrivals", body=my_index_body)
        elastic_logger.info("Index 'arrivals' created successfully.")
    except RequestError as e:
        if "resource_already_exists_exception" in str(e):
            elastic_logger.info("Index 'arrivals' already exists.")
        else:
            elastic_logger.error(f"Error creating index: {e}")


