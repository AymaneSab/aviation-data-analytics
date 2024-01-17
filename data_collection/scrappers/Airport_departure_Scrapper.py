import sys
import os
import logging
import datetime
import atexit

sys.path.append('/Users/sabri/Desktop/Study /Youcode/Github/aviation-data-analytics/data_collection/integrators/')

from DataBase_Controller import DBManager
from opensky_api import OpenSkyApi

class DepartureScrapper:

    def __init__(self, airport_icao, from_date_str, to_date_str, db_server, db_database, db_username, db_password):
        self.airport_icao = airport_icao
        self.from_date_str = from_date_str
        self.to_date_str = to_date_str
        self.api = OpenSkyApi()
        self.db_manager = DBManager(db_server, db_database, db_username, db_password)
        self.logger = self.setup_logging("Log/AirportDeparture_Scrapper" , "Log.log")
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

    def get_departures(self):
        try:
            table_name = "AirportDepartures"
            columns_definition = '''
                    icao24 NVARCHAR(24),
                    first_seen NVARCHAR(24),
                    est_departure_airport NVARCHAR(10),
                    last_seen NVARCHAR(24),
                    est_arrival_airport NVARCHAR(10),
                    callsign NVARCHAR(8),
                    est_departure_airport_horiz_distance NVARCHAR(24),
                    est_departure_airport_vert_distance NVARCHAR(24),
                    est_arrival_airport_horiz_distance NVARCHAR(24),
                    est_arrival_airport_vert_distance NVARCHAR(24),
                    departure_airport_candidates_count NVARCHAR(24),
                    arrival_airport_candidates_count NVARCHAR(24)
                '''
            # Create the table if it doesn't exist
            self.db_manager.create_table_if_not_exists(table_name, columns_definition)
            self.logger.info(f"{table_name} ----- Table created successfully ")

            # Convert date strings to Unix time
            from_date_unix = self._convert_to_unix_time(self.from_date_str)
            to_date_unix = self._convert_to_unix_time(self.to_date_str)

            departures = self.api.get_departures_by_airport(str(self.airport_icao), from_date_unix, to_date_unix)
            self.logger.info(f"Departure Data Returned Successfully: {len(departures)} departures found.")

            if departures:
                # Insert departures into the database
                for departure_data in departures:
                    icao24 = departure_data.icao24
                    first_seen = departure_data.firstSeen  # Correct attribute name
                    est_departure_airport = departure_data.estDepartureAirport
                    last_seen = departure_data.lastSeen  # Correct attribute name
                    est_arrival_airport = departure_data.estArrivalAirport
                    callsign = departure_data.callsign
                    est_departure_airport_horiz_distance = departure_data.estDepartureAirportHorizDistance
                    est_departure_airport_vert_distance = departure_data.estDepartureAirportVertDistance
                    est_arrival_airport_horiz_distance = departure_data.estArrivalAirportHorizDistance
                    est_arrival_airport_vert_distance = departure_data.estArrivalAirportVertDistance
                    departure_airport_candidates_count = departure_data.departureAirportCandidatesCount
                    arrival_airport_candidates_count = departure_data.arrivalAirportCandidatesCount

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
                self.logger.info(f"{len(departures)} departures saved to the database.")

        except Exception as e:
            self.logger.error(f"An error occurred: {e}")

        finally:
            self.db_manager.close()

# Example usage:
scrapper = DepartureScrapper('CYGW', '2023-01-15', '2023-01-20', '10.211.55.3', 'Flights_StagingArea_DB', 'Database_Administrator', 'AllahSave.1234/')
scrapper.get_departures()



        
