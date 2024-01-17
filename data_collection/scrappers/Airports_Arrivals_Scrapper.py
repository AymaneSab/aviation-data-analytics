import sys
import os
import logging
import datetime
import atexit

sys.path.append('/Users/sabri/Desktop/Study /Youcode/Github/aviation-data-analytics/data_collection/integrators/')

from DataBase_Controller import DBManager
from opensky_api import OpenSkyApi

class ArrivalsScrapper:

    def __init__(self, airport_icao, from_date_str, to_date_str, db_server, db_database, db_username, db_password):
        self.airport_icao = airport_icao
        self.from_date_str = from_date_str
        self.to_date_str = to_date_str
        self.logger = self.setup_logging("Log/Airports_arrivals_Scrapper", "Log.log")
        self.api = OpenSkyApi()
        self.db_manager = DBManager(db_server, db_database, db_username, db_password)
        self.db_manager.connect()

    def setup_logging(self, log_directory, logger_name):
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

    def _convert_to_unix_time(self, date_str):
        # Convert 'yyyy-MM-dd' to Unix time (seconds since epoch)
        date_object = datetime.datetime.strptime(date_str, '%Y-%m-%d')
        return int(date_object.timestamp())
    
    def get_arrivals(self):
        try:
            table_name = "AirportArrivals"
            columns_definition = '''
                    icao24 NVARCHAR(24),
                    first_seen INT,
                    est_departure_airport NVARCHAR(10),
                    last_seen INT,
                    est_arrival_airport NVARCHAR(10),
                    callsign NVARCHAR(8),
                    est_departure_airport_horiz_distance INT,
                    est_departure_airport_vert_distance INT,
                    est_arrival_airport_horiz_distance INT,
                    est_arrival_airport_vert_distance INT,
                    departure_airport_candidates_count INT,
                    arrival_airport_candidates_count INT
                '''
            # Create the table if it doesn't exist
            self.db_manager.create_table_if_not_exists(table_name, columns_definition)

            # Convert date strings to Unix time
            from_date_unix = self._convert_to_unix_time(self.from_date_str)
            to_date_unix = self._convert_to_unix_time(self.to_date_str)

            arrivals = self.api.get_arrivals_by_airport(str(self.airport_icao), from_date_unix, to_date_unix)
            self.logger.info(f"arrivals Data Returned Successfully: {len(arrivals)} arrivals found.")

            if arrivals:
                # Insert departures into the database
                for arrival_data in arrivals:
                    icao24 = arrival_data.icao24
                    first_seen = arrival_data.firstSeen  # Correct attribute name
                    est_departure_airport = arrival_data.estDepartureAirport
                    last_seen = arrival_data.lastSeen  # Correct attribute name
                    est_arrival_airport = arrival_data.estArrivalAirport
                    callsign = arrival_data.callsign
                    est_departure_airport_horiz_distance = arrival_data.estDepartureAirportHorizDistance
                    est_departure_airport_vert_distance = arrival_data.estDepartureAirportVertDistance
                    est_arrival_airport_horiz_distance = arrival_data.estArrivalAirportHorizDistance
                    est_arrival_airport_vert_distance = arrival_data.estArrivalAirportVertDistance
                    departure_airport_candidates_count = arrival_data.departureAirportCandidatesCount
                    arrival_airport_candidates_count = arrival_data.arrivalAirportCandidatesCount

                    values = (
                        icao24,
                        first_seen,
                        est_departure_airport,
                        last_seen,
                        est_arrival_airport,
                        callsign,
                        est_departure_airport_horiz_distance,
                        est_departure_airport_vert_distance,
                        est_arrival_airport_horiz_distance,
                        est_arrival_airport_vert_distance,
                        departure_airport_candidates_count,
                        arrival_airport_candidates_count
                    )
                    self.db_manager.insert_data(table_name, values)

                self.db_manager.connection.commit()
                self.logger.info(f"{len(arrivals)} departures saved to the database.")

        except Exception as e:
            self.logger.error(f"An error occurred: {e}")

        finally:
            self.db_manager.close()



