from pyspark.sql.functions import col, lit, from_unixtime, coalesce , to_date , date_format
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


from pyspark.sql.functions import col

from pyspark.sql import SparkSession
from pyspark.sql.functions import col , when

def clean_and_preprocess_airports_data(kafka_stream_df):
    try:
        # Convert 'movieId' to string
        transformed_df = kafka_stream_df.withColumn("iata", col("iata").cast("string"))
        transformed_df = transformed_df.withColumn("icao", col("icao").cast("string"))
        transformed_df = transformed_df.withColumn("name", col("name").cast("string"))
        transformed_df = transformed_df.withColumn("location", col("location").cast("string"))
        transformed_df = transformed_df.withColumn("latitude", col("latitude").cast("string"))
        transformed_df = transformed_df.withColumn("longitude", col("longitude").cast("string"))
        transformed_df = transformed_df.withColumn("city", col("city").cast("string"))
        transformed_df = transformed_df.withColumn("country", col("country").cast("string"))
        transformed_df = transformed_df.withColumn("phone", when((col("phone") == "\"NaN\"") | (col("phone").isNull()), "Not Filled").otherwise(col("phone").cast("string")))
        transformed_df = transformed_df.withColumn("website", when((col("website") == "\"NaN\""), "Not Filled").otherwise(col("website").cast("string")))
        return transformed_df

    except ValueError as e:
        # Handle the case where the date format is incorrect
        return None

def clean_and_preprocess_departures_data(kafka_stream_df):
    try:
        # Convert 'icao24' to string
        transformed_df = kafka_stream_df.withColumn("icao24", col("icao24").cast("string"))
        transformed_df = transformed_df.withColumn("first_seen", from_unixtime("first_seen").cast("string"))
        transformed_df = transformed_df.withColumn("est_departure_airport", col("est_departure_airport").cast("string"))
        transformed_df = transformed_df.withColumn("last_seen", from_unixtime("last_seen").cast("string"))
        transformed_df = transformed_df.withColumn("est_arrival_airport", when(col("est_arrival_airport").isNull() | (col("est_arrival_airport") == "NaN"), "Not Filled").otherwise(col("est_arrival_airport").cast("string")))
        transformed_df = transformed_df.withColumn("callsign", col("callsign").cast("string"))
        transformed_df = transformed_df.withColumn("est_departure_airport_horiz_distance", col("est_departure_airport_horiz_distance").cast("integer"))
        transformed_df = transformed_df.withColumn("est_departure_airport_vert_distance", col("est_departure_airport_vert_distance").cast("integer"))
        transformed_df = transformed_df.withColumn("est_arrival_airport_horiz_distance", when(col("est_arrival_airport_horiz_distance").isNull() | (col("est_arrival_airport_horiz_distance") == "NaN"), 0).otherwise(col("est_arrival_airport_horiz_distance").cast("integer")))
        transformed_df = transformed_df.withColumn("est_arrival_airport_vert_distance", when(col("est_arrival_airport_vert_distance").isNull() | (col("est_arrival_airport_vert_distance") == "NaN"), 0).otherwise(col("est_arrival_airport_vert_distance").cast("integer")))
        transformed_df = transformed_df.withColumn("departure_airport_candidates_count", col("departure_airport_candidates_count").cast("integer"))
        transformed_df = transformed_df.withColumn("arrival_airport_candidates_count", col("arrival_airport_candidates_count").cast("integer"))
        transformed_df = transformed_df.withColumn("date", col("date").cast("string"))
        transformed_df = transformed_df.withColumn("airport_name", col("airport_name").cast("string"))
        transformed_df = transformed_df.withColumn("airport_country", col("airport_country").cast("string"))
        transformed_df = transformed_df.withColumn("airport_city", col("airport_city").cast("string"))

        return transformed_df

    except ValueError:
        # Handle the case where the date format is incorrect
        return None

def clean_and_preprocess_arrivals_data(kafka_stream_df):
    try:
        # Convert 'icao24' to string
        transformed_df = kafka_stream_df.withColumn("icao24", col("icao24").cast("string"))
        transformed_df = transformed_df.withColumn("first_seen", from_unixtime("first_seen").cast("string"))
        transformed_df = transformed_df.withColumn("est_departure_airport", col("est_departure_airport").cast("string"))
        transformed_df = transformed_df.withColumn("last_seen", from_unixtime("last_seen").cast("string"))
        transformed_df = transformed_df.withColumn("est_arrival_airport", when(col("est_arrival_airport").isNull() | (col("est_arrival_airport") == "NaN"), "Not Filled").otherwise(col("est_arrival_airport").cast("string")))
        transformed_df = transformed_df.withColumn("callsign", col("callsign").cast("string"))
        transformed_df = transformed_df.withColumn("est_departure_airport_horiz_distance", col("est_departure_airport_horiz_distance").cast("integer"))
        transformed_df = transformed_df.withColumn("est_departure_airport_vert_distance", col("est_departure_airport_vert_distance").cast("integer"))
        transformed_df = transformed_df.withColumn("est_arrival_airport_horiz_distance", when(col("est_arrival_airport_horiz_distance").isNull() | (col("est_arrival_airport_horiz_distance") == "NaN"), 0).otherwise(col("est_arrival_airport_horiz_distance").cast("integer")))
        transformed_df = transformed_df.withColumn("est_arrival_airport_vert_distance", when(col("est_arrival_airport_vert_distance").isNull() | (col("est_arrival_airport_vert_distance") == "NaN"), 0).otherwise(col("est_arrival_airport_vert_distance").cast("integer")))
        transformed_df = transformed_df.withColumn("departure_airport_candidates_count", col("departure_airport_candidates_count").cast("integer"))
        transformed_df = transformed_df.withColumn("arrival_airport_candidates_count", col("arrival_airport_candidates_count").cast("integer"))
        transformed_df = transformed_df.withColumn("date", col("date").cast("string"))
        transformed_df = transformed_df.withColumn("airport_name", col("airport_name").cast("string"))
        transformed_df = transformed_df.withColumn("airport_country", col("airport_country").cast("string"))
        transformed_df = transformed_df.withColumn("airport_city", col("airport_city").cast("string"))

        return transformed_df

    except ValueError:
        # Handle the case where the date format is incorrect
        return None