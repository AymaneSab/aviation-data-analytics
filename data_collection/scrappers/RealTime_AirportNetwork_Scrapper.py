import os
import logging
import pandas as pd
from Airport_departure_Scrapper import DepartureScrapper
from Airports_Arrivals_Scrapper import ArrivalsScrapper
from datetime import datetime, timedelta
import json 
from datetime import datetime, timedelta
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

    def get_Density(self, country):
        try:
            cities = self.get_airport_cities(country)
            self.logger.info(f"Cities with airports: {cities}")

            icao_codes = self.get_icao_codes(cities)
            self.logger.info(f"ICAO codes: {icao_codes}")

            for icao_code in icao_codes:
                all_data = {"departures": {}, "arrivals": {}, "airport_info": {}}

                self.logger.info(f"Fetching data for airport with ICAO code: {icao_code}")

                # Fetch airport information from the CSV file
                airport_info_list = self.airports_data[self.airports_data['icao'] == icao_code].to_dict('records')
                
                self.logger.debug(f"Airport info list: {airport_info_list}")

                if airport_info_list:
                    airport_info = airport_info_list[0]
                    all_data["airport_info"] = airport_info
                    airport_name = all_data["airport_info"]["name"]  
                    airport_country = all_data["airport_info"]["country"]  # Extracting airport name
                    airport_city = all_data["airport_info"]["city"]  # Extracting airport name

                    self.logger.info(f"Airport name: {airport_name}")

                else:
                    self.logger.warning("No airport information found for the given ICAO code. Skipping processing.")

                # Calculate current time and 5 minutes from now
                start_time = "2023-01-30 03:00:00"
                end_time = '2023-02-02 04:00:00'
                
                # current_time = datetime.now()
                # start_time = current_time.replace(hour=0, minute=0, second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S')
                # end_time = (current_time + timedelta(days=3)).replace(hour=0, minute=0, second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S')
                
                self.logger.info(f"Fetching data for the time period: {start_time} to {end_time}")

                # Fetch departures
                departure_scrapper = DepartureScrapper(icao_code, str(start_time), str(end_time))
                try:
                    departure_json_data = departure_scrapper.get_departures()

                    # Check if departure_json_data is not None
                    if departure_json_data is not None:
                        departures_data = json.loads(departure_json_data)

                        # Assuming departures_data is a list, check if it's not empty before processing
                        if departures_data and isinstance(departures_data, list):
                            all_data["departures"] = departures_data
                            for departure in departures_data:
                                departure["date"] = start_time  # Add date attribute
                                departure["airport_name"] = airport_name  # Add airport_name attribute
                                departure["airport_country"] = airport_country  # Add airport_name attribute
                                departure["airport_city"] = airport_city  # Add airport_name attribute

                            self.logger.info(f"{len(departures_data)} Departure data processed successfully")
                        else:
                            self.logger.warning("No valid departure data found. Skipping processing.")

                except Exception as departure_error:
                    self.logger.error(f"An error occurred while processing departures: {str(departure_error)}")

                # Fetch arrivals
                arrival_scrapper = ArrivalsScrapper(icao_code, start_time, end_time)
                try:
                    arrival_json_data = arrival_scrapper.get_arrivals()

                    # Check if arrival_json_data is not None
                    if arrival_json_data is not None:
                        arrivals_data = json.loads(arrival_json_data)

                        # Assuming arrivals_data is a list, check if it's not empty before processing
                        if arrivals_data and isinstance(arrivals_data, list):
                            all_data["arrivals"] = arrivals_data
                            for arrival in arrivals_data:
                                arrival["date"] = start_time  # Add date attribute
                                arrival["airport_name"] = airport_name  # Add airport_name attribute
                                arrival["airport_country"] = airport_country  # Add airport_name attribute
                                arrival["airport_city"] = airport_city  # Add airport_name attribute

                            self.logger.info(f"{len(arrivals_data)} Arrival data processed successfully")

                        else:
                            self.logger.warning("No valid arrival data found. Skipping processing.")

                except Exception as arrival_error:
                    self.logger.error(f"An error occurred while processing arrivals: {str(arrival_error)}")
                    pass

                self.logger.info(f"all_data: {all_data}")

                yield json.dumps(all_data)

        except Exception as e:
            self.logger.error(f"An error occurred in get_Density: {str(e)}\n")

