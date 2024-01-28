import sys
import os
import logging
import datetime
import atexit
import json

sys.path.append("/home/hadoop/aviation-data-analytics/opensky-api/python")

from opensky_api import OpenSkyApi

class DepartureScrapper:

    def __init__(self, airport_icao, from_date_time_str, to_date_time_str):
        self.airport_icao = airport_icao
        self.from_date_time_str = from_date_time_str
        self.to_date_time_str = to_date_time_str
        self.api = OpenSkyApi()
        self.logger = self.setup_logging("Log/AirportDeparture_Scrapper", "Log.log")

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

    def _convert_to_unix_time(self, date_time_str):
        # Convert 'yyyy-MM-dd HH:mm:ss' to Unix time (seconds since epoch)
        date_object = datetime.datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S')
        return int(date_object.timestamp())

    def get_departures(self):
        try:
            # Convert date strings with time to Unix time
            from_date_time_unix = self._convert_to_unix_time(self.from_date_time_str)
            to_date_time_unix = self._convert_to_unix_time(self.to_date_time_str)

            departures = self.api.get_departures_by_airport(
                str(self.airport_icao), from_date_time_unix, to_date_time_unix
            )
            self.logger.info(f"Departure Data Returned Successfully: {len(departures)} departures found.")

            departures_data_list = []

            if departures:
                # Insert departures into the database
                for departure_data in departures:
                    icao24 = departure_data.icao24
                    first_seen = departure_data.firstSeen
                    est_departure_airport = departure_data.estDepartureAirport
                    last_seen = departure_data.lastSeen
                    est_arrival_airport = departure_data.estArrivalAirport
                    callsign = departure_data.callsign
                    est_departure_airport_horiz_distance = departure_data.estDepartureAirportHorizDistance
                    est_departure_airport_vert_distance = departure_data.estDepartureAirportVertDistance
                    est_arrival_airport_horiz_distance = departure_data.estArrivalAirportHorizDistance
                    est_arrival_airport_vert_distance = departure_data.estArrivalAirportVertDistance
                    departure_airport_candidates_count = departure_data.departureAirportCandidatesCount
                    arrival_airport_candidates_count = departure_data.arrivalAirportCandidatesCount

                    values = {
                        "icao24": icao24,
                        "first_seen": first_seen,
                        "est_departure_airport": est_departure_airport,
                        "last_seen": last_seen,
                        "est_arrival_airport": est_arrival_airport,
                        "callsign": callsign,
                        "est_departure_airport_horiz_distance": est_departure_airport_horiz_distance,
                        "est_departure_airport_vert_distance": est_departure_airport_vert_distance,
                        "est_arrival_airport_horiz_distance": est_arrival_airport_horiz_distance,
                        "est_arrival_airport_vert_distance": est_arrival_airport_vert_distance,
                        "departure_airport_candidates_count": departure_airport_candidates_count,
                        "arrival_airport_candidates_count": arrival_airport_candidates_count
                    }

                    departures_data_list.append(values)

            return json.dumps(departures_data_list)

        except Exception as e:
            self.logger.error(f"An error occurred: {e}")
