import sys
import os
import logging
import datetime
import atexit

sys.path.append('/Users/sabri/Desktop/Study /Youcode/Github/aviation-data-analytics/data_collection/integrators/')

from DataBase_Controller import DBManager
from opensky_api import OpenSkyApi

class DepartureScrapper:

    def __init__(self, airport_icao, from_date, to_date, db_server, db_database, db_username, db_password):
        self.airport_icao = airport_icao
        self.from_date = datetime.datetime.strptime(from_date, "%Y-%m-%d").timestamp()
        self.to_date = datetime.datetime.strptime(to_date, "%Y-%m-%d").timestamp()
        self.api = OpenSkyApi()
        self.db_manager = DBManager(db_server, db_database, db_username, db_password)
        self.logger  = DBManager.setup_logging(self)
        self.db_manager.connect()

    def get_departures(self):
        try:
            table_name = "AirportDepartures"
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
            self.logger.info(f"{table_name} ----- Table created succefully ")

            departures = self.api.get_departures_by_airport(str(self.airport_icao), int(self.from_date), int(self.to_date))
            if departures:
                # Insert departures into the database
                for departure in departures:
                    values = ', '.join([f"'{departure[key]}'" for key in departure.keys()])
                    self.db_manager.cursor.execute(f"INSERT INTO {table_name} VALUES ({values})")

                self.db_manager.connection.commit()
                self.logger.info(f"{len(departures)} departures saved to the database.")

        except Exception as e:
            self.logger.error(f"An error occurred: {e}")

        finally:
            self.db_manager.close()

# Example usage:
scrapper = DepartureScrapper('EDDF', '2024-01-10', '2024-01-11', '10.211.55.3', 'Flights_StagingArea_DB', 'Database_Administrator', 'AllahSave.1234/')

scrapper.get_departures()
