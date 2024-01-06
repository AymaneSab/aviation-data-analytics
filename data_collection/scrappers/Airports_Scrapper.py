import atexit
import datetime
import logging
import os
from hamcrest import none
import requests
import pandas as pd
import sys 

class Airport_Scrapper:

    def __init__(self):
        self.base_url = "https://airport-info.p.rapidapi.com/airport"
        self.api_key = "1533fb0b48msh2d3464ad1f07401p19b9c7jsn6b2da9ab4c3a"
        self.headers = {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": "airport-info.p.rapidapi.com"
        }
        self.csv_path = "data_collection/collected_data/airports_data.csv"

        self.logger = self.setup_logging()

    def setup_logging(self):
        log_directory = "Log/AirPorts_DataCollection"
        os.makedirs(log_directory, exist_ok=True)
        log_filename = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S.log")
        log_filepath = os.path.join(log_directory, log_filename)

        logging.basicConfig(filename=log_filepath, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

        consumer_logger = logging.getLogger(__name__)
        atexit.register(logging.shutdown)  # Ensure proper shutdown of the logger

        return consumer_logger

    def read_airports_iata(self):
        return pd.read_csv(self.csv_path)

    def fetch_airport_info(self, iata_code):
        url = f"{self.base_url}?iata={iata_code}"

        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()  # Vérifiez si la requête a réussi
            return response.json() if response.status_code == 200 else None
        
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Erreur de requête pour l'aéroport {iata_code}: {str(e)}")
            return None

    def scrape_airports_info(self):
        df = None  
        airport_info_list = []

        try:
            airports_data = self.read_airports_iata()

            for iata_code in airports_data['code']:
                airport_info = self.fetch_airport_info(iata_code)

                if airport_info:
                    self.logger.info(f"Informations ajoutées pour l'aéroport {iata_code}")
                    airport_info_list.append(airport_info)
                else:
                    self.logger.warning(f"Échec de la requête pour l'aéroport {iata_code}.")

            self.save_scrapped_airports(airport_info_list)

        except KeyboardInterrupt:
            self.logger.info("Programme interrompu par l'utilisateur....Saving the current data ")
            self.save_scrapped_airports(airport_info_list)

        except Exception as e:
            self.logger.error(f"Exception thrown at  scrape_airports_info()  : {str(e)}")
            self.save_scrapped_airports(airport_info_list)

        finally:
            self.save_scrapped_airports(airport_info_list)

    def save_scrapped_airports(self , airport_info_list):
        try:
            self.logger.info("Saving the scrapped airports data ........................................")

            df = pd.DataFrame(airport_info_list)[['iata', 'icao', 'name', 'location', 'latitude', 'longitude', 'website'  , 'country', 'country_iso', 'phone']]

            # Éliminez les lignes avec des valeurs manquantes dans l'un des champs
            df = df.dropna(subset=['iata', 'icao', 'name', 'location', 'latitude', 'longitude', 'website' ,'city', 'country', 'country_iso', 'phone'])

            df.to_csv("/Users/sabri/Desktop/Study /Youcode/Github/aviation-data-analytics/data_collection/collected_airports_data.csv", index=False)

            self.logger.info("Les données ont été sauvegardées dans 'collected_airports_data.csv'.")

        except Exception as e:
            self.logger.error(f"Exception thrown at save_scrapped_airports : {e} ")
            sys.exit(0)


scrapper = Airport_Scrapper()

scrapper.scrape_airports_info()