import atexit
import pyodbc
import pandas as pd
import sys
import os
from datetime import datetime
import logging

sys.path.append('/Users/sabri/Desktop/Study /Youcode/Github/aviation-data-analytics/data_collection/integrators/')
from DataBase_Controller import DBManager

class FlightsDataProcessor:

    def __init__(self, server, database, username, password):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.flightsData = pd.DataFrame()
        self.db_manager = DBManager(self.server, self.database, self.username, self.password)
        self.db_manager.connect()
        self.logger = self.setup_logging("Log/Flights_PreProcessing", "Flights_Processor.log")

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

    def filter_new_data(self, data):
        try:
            self.logger.info("Filtrage des nouvelles données à traiter.")
            last_execution_date = datetime.now()
            self.flightsData = data[data['date_lecture'] > last_execution_date].copy()
            self.logger.info("Filtrage des nouvelles données terminé avec succès.")
        except Exception as e:
            self.logger.exception(f"Une erreur s'est produite lors du filtrage des nouvelles données : {e}")
            raise e

    def read_from_database(self, table_name):
        try:
            self.logger.info("Lecture des données depuis la base de données.")
            query = f"SELECT * FROM {table_name};"
            self.flightsData = pd.read_sql_query(query, self.db_manager.connection)
            self.flightsData['date_lecture'] = datetime.now()
            self.logger.info(f"Données lues avec succès depuis la table {table_name}.")
        except Exception as e:
            self.logger.exception(f"Une erreur s'est produite lors de la lecture depuis la base de données : {e}")
            raise e

    def drop_columns(self, columns_to_drop):
        try:
            self.logger.info("Suppression des colonnes spécifiées du DataFrame.")
            self.flightsData = self.flightsData.drop(columns=columns_to_drop, errors='ignore')
            self.logger.info("Colonnes supprimées avec succès.")
        except Exception as e:
            self.logger.exception(f"Une erreur s'est produite lors de la suppression des colonnes : {e}")
            raise e

    def rename_columns(self,column_mapping):
        """Renommer les colonnes du DataFrame."""
        try:
            self.logger.info("Renommage des colonnes du DataFrame.")

            # Utiliser la méthode rename pour renommer les colonnes
            self.flightsData = self.flightsData.rename(columns=column_mapping, inplace=True)

            self.logger.info("Renommage des colonnes terminé avec succès.")

        except Exception as e:
            print(f"Une erreur s'est produite lors du renommage des colonnes : {e}")
            self.logger.exception(f"Une erreur s'est produite lors du renommage des colonnes : {e}")
            raise e
        
    def replace_nulls_in_column(self, column_to_process, new_value):
        try:
            self.logger.info(f"Remplacement des valeurs nulles dans la colonne {column_to_process} par la nouvelle valeur {new_value}.")
            self.logger.info(f"Nombre de valeurs nulles remplacées dans {column_to_process}: {self.flightsData[column_to_process].isnull().sum()}")
            self.flightsData[column_to_process] = self.flightsData[column_to_process].fillna(new_value)
            self.logger.info("Remplacement des valeurs nulles terminé avec succès.")
        except Exception as e:
            self.logger.exception(f"Une erreur s'est produite lors du remplacement des valeurs nulles dans la colonne : {e}")
            raise e

    def extract_currency_and_value(self, column_to_process):
        try:
            self.logger.info(f"Extraction de la devise et de la valeur depuis la colonne {column_to_process}.")
            pattern = r'(?P<currency>[A-Za-z]+)\s*(?P<value>[0-9,.]+)'
            extracted_data = self.flightsData[column_to_process].str.extract(pattern)
            self.flightsData[column_to_process + '_currency'] = extracted_data['currency']
            self.flightsData[column_to_process + '_value'] = extracted_data['value']
            self.logger.info("Extraction de la devise et de la valeur terminée avec succès.")
        except Exception as e:
            self.logger.exception(f"Une erreur s'est produite lors de l'extraction de la devise et de la valeur : {e}")
            raise e

    def extract_percentage_value(self, column_to_process):
        try:
            self.logger.info(f"Extraction de la valeur en pourcentage depuis la colonne {column_to_process}.")
            pattern = r'([-+]?\d*\.\d+|\d+)%'
            self.flightsData[column_to_process + '_percentage'] = self.flightsData[column_to_process].str.extract(pattern)
            self.logger.info("Extraction de la valeur en pourcentage terminée avec succès.")
        except Exception as e:
            self.logger.exception(f"Une erreur s'est produite lors de l'extraction de la valeur en pourcentage : {e}")
            raise e

    def split_hour_indicator(self, hour_columns):
        """Séparer l'heure et l'indicateur AM/PM dans deux colonnes distinctes et supprimer les colonnes d'origine."""
        try:
            self.logger.info("Séparation de l'heure et de l'indicateur AM/PM.")

            for column in hour_columns:
                # Vérifier si la colonne existe dans le DataFrame
                if column in self.flightsData.columns:
                    # Extraire l'heure et l'indicateur AM/PM
                    self.flightsData[column + '_heure'] = self.flightsData[column].apply(lambda x: x.split()[0] if isinstance(x, str) else x)
                    self.flightsData[column + '_indicateur'] = self.flightsData[column].apply(lambda x: x.split()[1] if isinstance(x, str) and len(x.split()) > 1 else '')
                    
                    # Supprimer la colonne d'origine
                    self.flightsData = self.flightsData.drop(columns=[column])
                    
                else:
                    self.logger.warning(f"La colonne {column} n'existe pas dans le DataFrame.")

            self.logger.info("Séparation de l'heure et de l'indicateur AM/PM terminée avec succès.")

        except Exception as e:
            print(f"Une erreur s'est produite lors de la séparation de l'heure et de l'indicateur AM/PM : {e}")
            self.logger.exception(f"Une erreur s'est produite lors de la séparation de l'heure et de l'indicateur AM/PM : {e}")
            raise e
        
    def fill_missing_prices(self):
        """Remplir les valeurs manquantes des prix en fonction de l'origine, de la destination et de la compagnie aérienne."""
        try:
            self.logger.info("Remplissage des valeurs manquantes des prix.")

            # Créer un masque pour les valeurs manquantes dans la colonne des prix
            missing_values_mask = self.flightsData['Price'].isnull()

            # Parcourir chaque ligne et remplacer les valeurs manquantes dans la colonne des prix
            for index, row in self.flightsData[missing_values_mask].iterrows():
                # Recherchez d'abord une ligne correspondante avec des valeurs non nulles pour l'origine, la destination et la compagnie aérienne
                matching_row = self.flightsData[
                    (self.flightsData['Origin'] == row['Origin']) &
                    (self.flightsData['Destination'] == row['Destination']) &
                    (self.flightsData['Airlines'] == row['Airlines']) &
                    (~self.flightsData['Price'].isnull())
                ].head(1)

                # Si aucune ligne correspondante n'est trouvée, recherchez une ligne basée uniquement sur l'origine et la destination
                if matching_row.empty:
                    matching_row = self.flightsData[
                        (self.flightsData['Origin'] == row['Origin']) &
                        (self.flightsData['Destination'] == row['Destination']) &
                        (~self.flightsData['Price'].isnull())
                    ].head(1)

                # Si une ligne correspondante est trouvée, remplacer la valeur manquante
                if not matching_row.empty:
                    self.flightsData.at[index, 'Price'] = matching_row.iloc[0]['Price']

            self.logger.info("Remplissage des valeurs manquantes des prix terminé avec succès.")

        except Exception as e:
            print(f"Une erreur générale s'est produite lors du remplissage des valeurs manquantes des prix : {e}")
            self.logger.exception(f"Une erreur générale s'est produite lors du remplissage des valeurs manquantes des prix : {e}")
            raise e
        
    def process_flights_data(self):
        try:
            self.logger.info("Initialisation du traitement des données sur les vols.")
            self.read_from_database("FlightsData")
            self.replace_nulls_in_column("LayoverTime", "0")
            self.replace_nulls_in_column("StopLocation", "No Stops")
            self.replace_nulls_in_column("TripType", "Not Defined")
            self.extract_currency_and_value('Price')
            self.extract_percentage_value('EmissionAvgDiff')
            self.fill_missing_prices()
            self.split_hour_indicator({"ArrivalTimeLeg1" , "DepartTimeLeg1"})
            self.drop_columns(["EmissionAvgDiff" , "Price" , "date_lecture"])

            # Renommer les colonnes
            columns_mapping = {'DepartTimeLeg1': 'DepartTime', 'ArrivalTimeLeg1': 'ArrivalTime'}
            self.rename_columns(columns_mapping)

            self.flightsData.to_csv("Flights_Data.csv")
            self.logger.info("Traitement des données sur les vols terminé avec succès.")

        except Exception as e:
            self.logger.exception(f"Une erreur s'est produite lors du traitement des données sur les vols : {e}")
            raise e

try:
    processor = FlightsDataProcessor('10.211.55.3', 'Flights_StagingArea_DB', 'Database_Administrator', 'AllahSave.1234/')
    processor.process_flights_data()

except Exception as e:
    print(f"Une erreur s'est produite : {e}")
