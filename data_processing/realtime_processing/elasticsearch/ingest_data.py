import time
import findspark
findspark.init()

import logging
from datetime import datetime
import threading
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType , IntegerType ,  LongType
from pyspark.sql.streaming import StreamingQueryException
import os 
import sys 
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from pyspark.sql.functions import col,from_json, explode , lit
from pyspark.sql import functions as F

from create_indices import connectToelastic, createAirportsIndex, createDeparturesIndex, createArrivalsIndex
from clean_data import clean_and_preprocess_airports_data, clean_and_preprocess_departures_data, clean_and_preprocess_arrivals_data


def setup_logging(log_directory, logger_name):
    os.makedirs(log_directory, exist_ok=True)

    log_filename = datetime.now().strftime("%Y-%m-%d_%H-%M-%S.log")
    log_filepath = os.path.join(log_directory, log_filename)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    handler = logging.FileHandler(log_filepath)
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    return logger

def setup_sparkSessionInitialiser_logging():
    return setup_logging("Log/SparkStreaming", "Spark_Sessions")

def setup_ElasticSearchConnection_logging():
    return setup_logging("Log/ElasticSearch", "ElasticSearchReequests")

def setup_sparkTreatment_airports_logging():
    return setup_logging("Log/Kafka_Airports_Insertion", "sparkTreatment_airports")

def setup_sparkTreatment_departures_logging():
    return setup_logging("Log/Kafka_Departures_Insertion", "sparkTreatment_departures")

def setup_sparkTreatment_arrivals_logging():
    return setup_logging("Log/Kafka_Arrivals_Insertion", "sparkTreatment_arrivals")

def setup_main_logging():
    return setup_logging("Log/Main_Script", "main")

def sparkSessionInitialiser(logger):
        packages = [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
            "org.elasticsearch:elasticsearch-spark-30_2.12:8.4.2",
        ]
        logger.info("Packages Loaded Succefully ")

        # Initialize SparkSession for Elasticsearch
        spark = SparkSession.builder \
            .appName("Spark Treatment") \
            .config("spark.jars.packages", ",".join(packages)) \
            .getOrCreate()
        
        logger.info("Spark Session Returned Succefully ")
        
        return spark

def sparkTreatment_airports(topicname, kafka_bootstrap_servers , spark_logger , movies_logger):
    try:
        spark = sparkSessionInitialiser(spark_logger)

        movies_logger.info("sparkTreatment_airports")

        # Define the schema for Kafka messages

        # Define the Kafka message schema
        kafka_schema = StructType([
            StructField("iata", StringType(), True),
            StructField("icao", StringType(), True),
            StructField("name", StringType(), True),
            StructField("location", StringType(), True),
            StructField("latitude", StringType(), True),
            StructField("longitude", StringType(), True),
            StructField("website", StringType(), True),
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("country_iso", StringType(), True),
            StructField("phone", StringType(), True)
        ])


        # Read data from Kafka topic with defined schema
        kafka_stream_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", topicname) \
            .load() \
            .selectExpr("CAST(value AS STRING)") \
            .select(from_json("value", kafka_schema).alias("data")) \
            .select("data.*")
        
        movies_logger.info(f"Data Loaded From {topicname} Topic Succefully ")
        
        try :

            treated_airports = clean_and_preprocess_airports_data(kafka_stream_df)
            movies_logger.info(f"treated_airports : {treated_airports}")

            movies_logger.info("Transformation Succefull")

        except Exception as e :
            movies_logger.error(f"Transformation Failed {str(e)}")
            pass

        checkpoint_location = "Elasticsearch/Checkpoint/airports"

        try:
            if not os.path.exists(checkpoint_location):
                os.makedirs(checkpoint_location)

            treated_airports.writeStream \
                .format("org.elasticsearch.spark.sql") \
                .outputMode("append") \
                .option("es.nodes", "localhost") \
                .option("es.port", "9200") \
                .option("es.resource", "airports") \
                .option("checkpointLocation", checkpoint_location) \
                .start().awaitTermination()

            movies_logger.info("treated_airports Sent To Elastic ")

        except Exception as e:
            movies_logger.error(f"An error occurred inawait  terminnatin: {str(e)}")
            pass
        
    except Exception as e:
        movies_logger.error(f"An error occurred: {str(e)}")
        pass

def sparkTreatment_departures(topicname, kafka_bootstrap_servers, spark_logger, user_logger):
    try:
        # Initialize Spark session
        session =  sparkSessionInitialiser(spark_logger)
        user_logger.info("----------> Packages Loaded Successfully ")

        # Define the schema for Kafka messages
        kafka_schema = StructType([
            StructField("icao24", StringType(), True),
            StructField("first_seen", StringType(), True),
            StructField("est_departure_airport", StringType(), True),
            StructField("last_seen", StringType(), True),
            StructField("est_arrival_airport", StringType(), True),
            StructField("callsign", StringType(), True),
            StructField("est_departure_airport_horiz_distance", IntegerType(), True),
            StructField("est_departure_airport_vert_distance", IntegerType(), True),
            StructField("est_arrival_airport_horiz_distance", IntegerType(), True),
            StructField("est_arrival_airport_vert_distance", IntegerType(), True),
            StructField("departure_airport_candidates_count", IntegerType(), True),
            StructField("arrival_airport_candidates_count", IntegerType(), True),
            StructField("date", StringType(), True),
            StructField("airport_name", StringType(), True),
            StructField("airport_country", StringType(), True),
            StructField("airport_city", StringType(), True)
        ])


        # Read data from Kafka topic with defined schema
        df = session \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", topicname) \
            .load() \
            .selectExpr("CAST(value AS STRING)") \
            .select(explode(from_json("value", ArrayType(kafka_schema))).alias("data")) \
            .select("data.*")

        try :

            treated_departures = clean_and_preprocess_departures_data(df)
            movies_logger.info(f"treated_airports : {treated_departures}")

            user_logger.info("Transformation Succefull")

        except Exception as e :
            user_logger.error(f"Transformation Failed {str(e)}")

        checkpoint_location = "Elasticsearch/Checkpoint/Departures"

        try:
            if not os.path.exists(checkpoint_location):
                os.makedirs(checkpoint_location)

            treated_departures.writeStream \
                .format("org.elasticsearch.spark.sql") \
                .outputMode("append") \
                .option("es.nodes", "localhost") \
                .option("es.port", "9200") \
                .option("es.resource", "departures") \
                .option("checkpointLocation", checkpoint_location) \
                .start().awaitTermination()

        except Exception as e:
            user_logger.error(f"An error occurred inawait  terminnatin: {str(e)}")
            pass

    except StreamingQueryException as sqe:
        user_logger.error(f"Streaming query exception: {str(sqe)}")
        pass
    except Exception as e:
        user_logger.error(f"An error occurred: {str(e)}")
        pass
    finally:
        session.stop()

def sparkTreatment_arrivals(topicname, kafka_bootstrap_servers, spark_logger, user_logger):
    try:
        # Initialize Spark session
        session =  sparkSessionInitialiser(spark_logger)
        user_logger.info("----------> Packages Loaded Successfully ")

        # Define the schema for Kafka messages
        kafka_schema = StructType([
            StructField("icao24", StringType(), True),
            StructField("first_seen", StringType(), True),
            StructField("est_departure_airport", StringType(), True),
            StructField("last_seen", StringType(), True),
            StructField("est_arrival_airport", StringType(), True),
            StructField("callsign", StringType(), True),
            StructField("est_departure_airport_horiz_distance", StringType(), True),
            StructField("est_departure_airport_vert_distance", StringType(), True),
            StructField("est_arrival_airport_horiz_distance", StringType(), True),
            StructField("est_arrival_airport_vert_distance", StringType(), True),
            StructField("departure_airport_candidates_count", StringType(), True),
            StructField("arrival_airport_candidates_count", StringType(), True),
            StructField("date", StringType(), True),
            StructField("airport_name", StringType(), True),
            StructField("airport_country", StringType(), True),
            StructField("airport_city", StringType(), True)
        ])

        # Read data from Kafka topic with defined schema
        df = session \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", topicname) \
            .load() \
            .selectExpr("CAST(value AS STRING)") \
            .select(explode(from_json("value", ArrayType(kafka_schema))).alias("data")) \
            .select("data.*")

        try :

            treated_arrivals = clean_and_preprocess_arrivals_data(df)
            movies_logger.info(f"treated_airports : {treated_arrivals}")
            user_logger.info("Transformation Succefull")

        except Exception as e :
            user_logger.error(f"Transformation Failed {str(e)}")

        checkpoint_location = "Elasticsearch/Checkpoint/Arrivals"

        if not os.path.exists(checkpoint_location):
            os.makedirs(checkpoint_location)

        treated_arrivals.writeStream \
            .format("org.elasticsearch.spark.sql") \
            .outputMode("append") \
            .option("es.nodes", "localhost") \
            .option("es.port", "9200") \
            .option("es.resource", "arrivals") \
            .option("checkpointLocation", checkpoint_location) \
            .start().awaitTermination()
        
        user_logger.info("treated_arrivals Sent To Elastic ")

    except StreamingQueryException as sqe:
        user_logger.error(f"Streaming query exception: {str(sqe)}")
        pass
    except Exception as e:
        user_logger.error(f"An error occurred: {str(e)}")
        pass
    finally:
        session.stop()

def write_airports_to_elasticsearch(df, epoch_id,session, logger):
    try:
        es = connectToelastic(logger)

        # Write the batch data to Elasticsearch
        df.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", "localhost") \
            .option("es.port", "9200") \
            .option("es.resource", "airports/_doc") \
            .option("es.write.operation", "upsert") \
            .option("es.mapping.id", "icao") \
            .mode("append") \
            .save()
        
        time.sleep(2)
        
    except Exception as e:
        # Handle the exception (print or log the error message)
        logger.error(f"Error in write_to_elasticsearch: {str(e)}")

    finally :
        es.transport.close()

def write_departures_to_elasticsearch(df, epoch_id, session, logger):
    try:
        es = connectToelastic(user_logger)

        # Write the batch data to Elasticsearch
        df.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", "localhost") \
            .option("es.port", "9200") \
            .option("es.resource", "departures/_doc") \
            .option("es.write.operation", "upsert") \
            .option("es.mapping.id", "icao24") \
            .mode("append") \
            .save()
        
    except Exception as e:
        # Handle the exception (print or log the error message)
        logger.error(f"Error in write_to_elasticsearch: {str(e)}")
    finally : 
        es.transport.close()

def write_arrivals_to_elasticsearch(df, epoch_id, session, logger):
    try:
        es = connectToelastic(user_logger)

        # Write the batch data to Elasticsearch
        df.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", "localhost") \
            .option("es.port", "9200") \
            .option("es.resource", "arrivals/_doc") \
            .option("es.write.operation", "upsert") \
            .option("es.mapping.id", "icao24") \
            .mode("append") \
            .save()
        
    except Exception as e:
        # Handle the exception (print or log the error message)
        logger.error(f"Error in write_to_elasticsearch: {str(e)}")
    finally : 
        es.transport.close()

def runSparkTreatment(spark_logger ,elastic_logger, movies_logger , review_logger , user_logger , main_loggin):

    try:

        es = connectToelastic(elastic_logger)

        # createAirportsIndex(es, elastic_logger)
        # createDeparturesIndex(es , elastic_logger)
        # createArrivalsIndex(es, elastic_logger)

        # Create threads for sparkTreatment_movies and sparkTreatment_reviews
        movies_thread = threading.Thread(target=sparkTreatment_airports, args=("airports_topic", "localhost:9092" , spark_logger , movies_logger))
        reviews_thread = threading.Thread(target=sparkTreatment_departures, args=("departures_topic", "localhost:9092" , spark_logger , review_logger))
        user_thread = threading.Thread(target=sparkTreatment_arrivals, args=("arrivals_topic", "localhost:9092" ,spark_logger, user_logger))

        # Start the threads
        movies_thread.start()
        reviews_thread.start()
        user_thread.start()

        # Wait for both threads to finish
        movies_thread.join()
        reviews_thread.join()
        user_thread.join()

    except KeyboardInterrupt:
        main_loggin.info("Spark Treatment Stopped")
        pass
    except Exception as e:
        main_loggin.error(f"An unexpected error occurred: {e}")
        main_loggin.exception("An unexpected error occurred in Spark")
        pass


spark_logger    = setup_sparkSessionInitialiser_logging()  
elastic_logger  = setup_ElasticSearchConnection_logging()
movies_logger   = setup_sparkTreatment_airports_logging()
review_logger   = setup_sparkTreatment_departures_logging()
user_logger     = setup_sparkTreatment_arrivals_logging()
main_loggin     = setup_main_logging()


runSparkTreatment(spark_logger , elastic_logger ,movies_logger ,review_logger , user_logger , main_loggin)       
