import atexit
import os
import logging
import datetime
import pandas as pd
import sys
from flights_data import _Scrape


sys.path.append('/Users/sabri/Desktop/Study /Youcode/Github/aviation-data-analytics/data_collection/integrators/')

from DataBase_Controller import DBManager

class Flights_Scrapper:
    
    def __init__(self, airports_csv_path, db_manager):
        self.scraper = _Scrape()
        self.airports_data = pd.read_csv(airports_csv_path)
        self.logger = self.setup_logging()
        self.db_manager = db_manager

    def setup_logging(self):
        log_directory = "Log/Flights_Scrapper"
        os.makedirs(log_directory, exist_ok=True)
        log_filename = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S.log")
        log_filepath = os.path.join(log_directory, log_filename)

        logging.basicConfig(filename=log_filepath, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

        logger = logging.getLogger(__name__)
        atexit.register(logging.shutdown) 

        return logger

    def collect_flights_data(self, from_country, to_country, start_date, end_date):
        try:
            departure_cities = self.get_airport_cities(from_country)
            self.logger.info(f"Departure cities with airports: {departure_cities}")

            destination_cities = self.get_airport_cities(to_country)
            self.logger.info(f"Destination cities with airports: {destination_cities}")

            for departure_city in departure_cities:
                for destination_city in destination_cities:
                    flights_data = self.scraper(departure_city, destination_city, start_date, end_date).data  # Access the 'data' attribute
                    if flights_data:
                        self.logger.info(f"Flight data for {departure_city} to {destination_city}: {flights_data}")
                        table_name = 'FlightsData'
                        columns_definition = '''
                            LeaveDate VARCHAR(255),
                            ReturnDate VARCHAR(255),
                            DepartTimeLeg1 VARCHAR(255),
                            ArrivalTimeLeg1 VARCHAR(255),
                            Airlines VARCHAR(255),
                            TravelTime VARCHAR(255),
                            Origin VARCHAR(255),
                            Destination VARCHAR(255),
                            NumStops VARCHAR(255),
                            LayoverTime VARCHAR(255),
                            StopLocation VARCHAR(255),
                            CO2Emission VARCHAR(255),
                            EmissionAvgDiff VARCHAR(255),
                            Price VARCHAR(255),
                            TripType VARCHAR(255),
                            AccessDate VARCHAR(255)
                        '''
                        self.db_manager.create_table_if_not_exists(table_name, columns_definition)

                        for data_point in flights_data:
                            self.logger.info(data_point)
                            values = (
                                data_point['Leave Date'],
                                data_point['Return Date'],
                                data_point['Depart Time (Leg 1)'],
                                data_point['Arrival Time (Leg 1)'],
                                data_point['Airline(s)'],
                                data_point['Travel Time'],
                                data_point['Origin'],
                                data_point['Destination'],
                                data_point['Num Stops'],
                                data_point['Layover Time'],
                                data_point['Stop Location'],
                                data_point['CO2 Emission'],
                                data_point['Emission Avg Diff (%)'],
                                data_point['Price ($)'],
                                data_point['Trip Type'],
                                data_point['Access Date']
                            )
                            self.db_manager.insert_data(table_name, values)
                    else:
                        self.logger.warning(f"No flight data found for {departure_city} to {destination_city}")


            self.logger.info("Flight data collection completed. Result:")

        except Exception as e:
            self.logger.error(f"An error occurred during flight data collection: {str(e)}")

    def get_airport_cities(self, country):
        try:
            country_airports = self.airports_data[self.airports_data['country'] == country]
            airport_cities = country_airports['city'].tolist()
            return airport_cities
        
        except Exception as e:
            self.logger.error(f"An error occurred while retrieving cities with airports: {str(e)}")
            return []

# Usage
try:
    airports_csv_path = 'data_processing/Treated_data/treated_airport_data.csv'
    
    db_manager = DBManager('10.211.55.3', 'Flights_StagingArea_DB', 'Database_Administrator', 'AllahSave.1234/')
    db_manager.connect()

    flights_scrapper = Flights_Scrapper(airports_csv_path, db_manager)

    flights_scrapper.collect_flights_data('Morocco', 'France', '2024-01-15', '2024-01-17')

except Exception as e:
    print(f"{e}")
finally:
    db_manager.close()
