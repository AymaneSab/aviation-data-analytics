import atexit
import os
import logging
from datetime import datetime, timedelta 
import pandas as pd
import sys
from flights_data import _Scrape

sys.path.append('/home/hadoop/aviation-data-analytics/data_collection/integrators/')

from DataBase_Controller import DBManager

class Flights_Scrapper:
    
    def __init__(self, airports_csv_path, db_manager):
        self.scraper = _Scrape()
        self.airports_data = pd.read_csv(airports_csv_path)
        self.logger = self.setup_FlightsScrapper_logging("Log/Flights_Scrapper" , "Batch_Flights_Scrapper.log")
        self.db_manager = db_manager

    def setup_FlightsScrapper_logging(self, log_directory, logger_name):
        os.makedirs(log_directory, exist_ok=True)

        log_filename = logger_name
        log_filepath = os.path.join(log_directory, log_filename)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        handler = logging.FileHandler(log_filepath)
        handler.setLevel(logging.INFO)
        handler.setFormatter(formatter)

        logger = logging.getLogger(log_filename)
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)

        return logger

    def collect_flights_data(self, from_country, to_country, start_date, end_date):
        try:
            table_name = 'FlightsData'
            columns_definition = '''
                LeaveDate VARCHAR(255),
                ReturnDate VARCHAR(255),
                DepartTimeLeg1 VARCHAR(255),
                ArrivalTimeLeg1 VARCHAR(255),
                Airlines VARCHAR(255),
                TravelTime VARCHAR(255),
                Origin_City VARCHAR(255),
                Origin_Country VARCHAR(255),
                Destination_City VARCHAR(255),
                Destination_Country VARCHAR(255),
                NumStops VARCHAR(255),
                LayoverTime VARCHAR(255),
                StopLocation VARCHAR(255),
                CO2Emission VARCHAR(255),
                EmissionAvgDiff VARCHAR(255),
                Price VARCHAR(255),
                TripType VARCHAR(255),
                AccessDate VARCHAR(255)
            '''
            self.create_table(table_name ,columns_definition )

            departure_cities = self.get_airport_cities(from_country)
            self.logger.info(f"Départ des villes avec des aéroports : {departure_cities}")

            destination_cities = self.get_airport_cities(to_country)
            self.logger.info(f"Villes de destination avec des aéroports : {destination_cities}")

            for departure_city in departure_cities:
                for destination_city in destination_cities:
                    self.logger.info(f"{departure_city}----------{destination_city}")

                    flights_data = self.scraper(departure_city, destination_city, start_date, end_date).data  
                    
                    self.logger.info(f"flights_data----------{flights_data}")

                    if flights_data:

                        for data_point in flights_data:
                            values = (
                                data_point['Leave Date'],
                                data_point['Return Date'],
                                data_point['Depart Time (Leg 1)'],
                                data_point['Arrival Time (Leg 1)'],
                                data_point['Airline(s)'],
                                data_point['Travel Time'],
                                departure_city,
                                from_country,
                                destination_city,
                                to_country ,
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

            self.logger.info("Collecte des données de vol terminée. Résultat : \n")

        except Exception as e:
            self.logger.error(f"Une erreur s'est produite pendant la collecte des données de vol : {str(e)}\n")
            raise

    def get_airport_cities(self, country):
        try:
            country_airports = self.airports_data[self.airports_data['country'] == country]
            airport_cities = country_airports['city'].tolist()
            return airport_cities
        
        except Exception as e:
            self.logger.error(f"Une erreur s'est produite lors de la récupération des villes avec des aéroports : {str(e)}\n")
            return []

    def get_icao_codes(self, city_list):
        icao_codes = []
        for city in city_list:
            airport_info = self.airports_data[self.airports_data['city'] == city]
            if not airport_info.empty:
                icao_code = airport_info.iloc[0]['icao']
                icao_codes.append(icao_code)
        return icao_codes

    def create_table(self , table_name , columns_definition):
        self.db_manager.create_table_if_not_exists(table_name, columns_definition)

try:
    airports_csv_path = 'data_processing/Treated_data/treated_airport_data.csv'
    
    db_manager = DBManager('10.211.55.3', 'Flights_StagingArea_DB', 'Database_Administrator', 'AllahSave.1234/')
    db_manager.connect()

    flights_scrapper = Flights_Scrapper(airports_csv_path, db_manager)

    flights_scrapper.collect_flights_data('Morocco', 'France', '2024-01-30', '2024-02-05')

except Exception as e:
    print(f"{e}")

finally:
    db_manager.close()

