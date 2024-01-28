import os
import logging
import pandas as pd
from Airport_departure_Scrapper import DepartureScrapper
from Airports_Arrivals_Scrapper import ArrivalsScrapper
from datetime import datetime, timedelta
import json 

class Airports_Network_Scrapper:
    
    def __init__(self, airports_csv_path):
        self.airports_data = pd.read_csv(airports_csv_path)
        self.logger = self.setup_NetworkScrapper_logging("Log/Network_Scrapper", "Network.log")

    def setup_NetworkScrapper_logging(self, log_directory, logger_name):
        os.makedirs(log_directory, exist_ok=True)

        log_filename = logger_name
        log_filepath = os.path.join(log_directory, log_filename)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        handler = logging.FileHandler(log_filepath)
        handler.setLevel(logging.INFO)
        handler.setFormatter(formatter)

        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)

        return logger

    def get_airport_cities(self, country):
        try:
            country_airports = self.airports_data[self.airports_data['country'] == country]
            
            # Filter out NaN and empty values
            airport_cities = country_airports['city'].dropna().tolist()
            return airport_cities
            
        except Exception as e:
            self.logger.error(f"An error occurred while retrieving cities with airports: {str(e)}\n")
            return []
        
    def get_icao_codes(self, city_list):
        icao_codes = []
        for city in city_list:
            airport_info = self.airports_data[self.airports_data['city'] == city]

            if not airport_info.empty:
                icao_code = airport_info.iloc[0]['icao']
                icao_codes.append(icao_code)

        return icao_codes 

    def produce_to_kafka(self, table_name, columns_definition):
        try:
            # Assuming you have actual logic for Kafka production here
            pass

        except Exception as e:
            self.logger.error(f"An error occurred in produce_to_kafka: {str(e)}\n")

    def get_Density(self, country):
        try:
            cities = self.get_airport_cities(country)
            self.logger.info(f"Cities with airports: {cities}")

            icao_codes = self.get_icao_codes(cities)
            self.logger.info(f"ICAO codes: {icao_codes}")

            all_data = {"departures": {}, "arrivals": {}}

            for icao_code in icao_codes:
                # Calculate current time and 5 minutes from now
                # current_time = datetime.now()
                # start_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
                # end_time = (current_time + timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')

                start_time = "2023-01-29 03:00:00"
                end_time = '2023-01-30 04:00:00'

                # Fetch departures
                departure_scrapper = DepartureScrapper(icao_code, start_time, end_time)
                departure_json_data = departure_scrapper.get_departures()

                # Fetch arrivals
                arrival_scrapper = ArrivalsScrapper(icao_code, start_time, end_time)
                arrival_json_data = arrival_scrapper.get_arrivals()

                # Check if departure_json_data is not None before trying to load as JSON
                if departure_json_data is not None:
                    all_data["departures"][icao_code] = json.loads(departure_json_data)

                # Check if arrival_json_data is not None before trying to load as JSON
                if arrival_json_data is not None:
                    all_data["arrivals"][icao_code] = json.loads(arrival_json_data)

            yield json.dumps(all_data)

        except Exception as e:
            self.logger.error(f"An error occurred in get_Density: {str(e)}\n")

