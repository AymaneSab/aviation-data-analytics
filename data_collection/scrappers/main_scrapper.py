import atexit
import os
import logging
import datetime
import pandas as pd
from flights_data import _Scrape

class Flights_Scrapper:
    def __init__(self, airports_csv_path):
        self.scraper = _Scrape()
        self.airports_data = pd.read_csv(airports_csv_path)
        self.logger = self.setup_logging()

    def setup_logging(self):
        log_directory = "Log/Main_Scrapper"
        os.makedirs(log_directory, exist_ok=True)
        log_filename = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S.log")
        log_filepath = os.path.join(log_directory, log_filename)

        logging.basicConfig(filename=log_filepath, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

        logger = logging.getLogger("Flights_Scrapper")  # Utilisez un nom de logger spécifique pour votre classe
        atexit.register(logging.shutdown)  # Assurez-vous de fermer correctement le journal

        return logger

    def collect_flights_data(self, from_country, to_country, start_date, end_date):
        try:
            # Obtenez la liste des villes avec des aéroports pour le pays de départ
            departure_cities = self.get_airport_cities(from_country)
            self.logger.info(f"Villes de départ avec des aéroports : {departure_cities}")

            # Obtenez la liste des villes avec des aéroports pour le pays de destination
            destination_cities = self.get_airport_cities(to_country)
            self.logger.info(f"Villes de destination avec des aéroports : {destination_cities}")

            # Collectez les données de vol pour chaque combinaison de ville de départ et de destination
            all_flights_data = []
            for departure_city in departure_cities:
                for destination_city in destination_cities:
                    self.logger.info(f"{departure_city}:{destination_city}")
                    flights_data = self.scraper(departure_city, destination_city, start_date, end_date)
                    
                    if flights_data:
                        self.logger.info(f"Données de vol pour {departure_city} vers {destination_city} : {flights_data}")
                        all_flights_data.extend(flights_data)
                    else:
                        self.logger.warning(f"Aucune donnée de vol trouvée pour {departure_city} vers {destination_city}")

            if all_flights_data:
                self.logger.info("La collecte des données de vol est terminée. Résultat :")
                self.logger.info(all_flights_data)

            return all_flights_data

        except Exception as e:
            self.logger.error(f"Une erreur s'est produite pendant la collecte des données de vol : {str(e)}")
            raise

    def get_airport_cities(self, country):
        try:
            # Filtrer le DataFrame pour obtenir les villes avec des aéroports pour le pays donné
            country_airports = self.airports_data[self.airports_data['country'] == country]
            airport_cities = country_airports['city'].tolist()
            return airport_cities
        except Exception as e:
            self.logger.error(f"Une erreur s'est produite pendant la récupération des villes avec des aéroports : {str(e)}")
            return []


try:
    airports_csv_path = '/Users/sabri/Desktop/Study /Youcode/Github/aviation-data-analytics/data_collection/data_sources/airport_info.csv'
    flights_scrapper = Flights_Scrapper(airports_csv_path)
    result = flights_scrapper.collect_flights_data('Canada', 'United States', '2024-01-01', '2024-01-05')
    print(result)
    
except Exception as e:
    print(f"{e}")