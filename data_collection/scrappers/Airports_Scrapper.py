import atexit
import datetime
import logging
import os
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
        self.csv_path = "data_collection/data_sources/airports.csv"

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
        df = None  # Initialisez df en dehors du bloc try

        try:
            airports_data = self.read_airports_iata()

            if 'code' in airports_data.columns:
                airport_info_list = []

                for iata_code in airports_data['code']:
                    airport_info = self.fetch_airport_info(iata_code)

                    if airport_info:
                        self.logger.info(f"Informations ajoutées pour l'aéroport {iata_code}")
                        airport_info_list.append(airport_info)
                    else:
                        self.logger.warning(f"Échec de la requête pour l'aéroport {iata_code}.")

                df = pd.DataFrame(airport_info_list)[['iata', 'icao', 'name', 'location', 'latitude', 'longitude', 'website'  , 'country', 'country_iso', 'phone']]

                # Éliminez les lignes avec des valeurs manquantes dans l'un des champs
                df = df.dropna(subset=['iata', 'icao', 'name', 'location', 'latitude', 'longitude', 'website' ,'city', 'country', 'country_iso', 'phone'])

                df.to_csv("data_collection/data_sources/airport_info.csv", index=False)

                self.logger.info("Les données ont été sauvegardées dans 'airport_info.csv'.")

            else:
                self.logger.error("La colonne 'code' n'existe pas dans le DataFrame.")

        except KeyboardInterrupt:
            df = pd.DataFrame(airport_info_list)[['iata', 'icao', 'name', 'location', 'latitude', 'longitude', 'website' ,'city'  , 'country', 'country_iso', 'phone']]
            # Éliminez les lignes avec des valeurs manquantes dans l'un des champs
            df = df.dropna(subset=['iata', 'icao', 'name', 'location', 'latitude', 'longitude', 'website','city', 'country', 'country_iso', 'phone'])
            df.to_csv("data_collection/data_sources/airport_info.csv", index=False)
            self.logger.info("Programme interrompu par l'utilisateur....Saving the current data ")
            sys.exit(0)

        except Exception as e:
            self.logger.error(f"Erreur lors de la sauvegarde des données collectées : {str(e)}")

        finally:
            df = pd.DataFrame(airport_info_list)[['iata', 'icao', 'name', 'location', 'latitude', 'longitude', 'website','city', 'country', 'country_iso', 'phone']]
            df = df.dropna(subset=['iata', 'icao', 'name', 'location', 'latitude', 'longitude', 'website','city', 'country', 'country_iso', 'phone'])
            df.to_csv("data_collection/data_sources/airport_info.csv", index=False)
            self.logger.info("Programme terminé.")


scrapper = Airport_Scrapper()

scrapper.scrape_airports_info()