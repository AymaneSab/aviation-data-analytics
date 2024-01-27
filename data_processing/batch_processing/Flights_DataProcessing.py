import atexit
import pyodbc
import pandas as pd
import sys
import os
from datetime import datetime
import logging
import numpy as np
import  re 

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

        self.logger = self.setup_logging("Log/Flights_PreProcessing", "Flights_DataProcessor.log")

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
            new_data = data[data['date_lecture'] > last_execution_date].copy()

            self.logger.info("Filtrage des nouvelles données terminé avec succès.")

            return new_data

        except Exception as e:
            print(f"Une erreur s'est produite lors du filtrage des nouvelles données : {e}")
            self.logger.exception(f"Une erreur s'est produite lors du filtrage des nouvelles données : {e}")
            raise e

    def read_from_database(self, table_name):
        try:
            self.logger.info("Lecture des données depuis la base de données.")
            query = f"SELECT * FROM {table_name};"
            self.flightsData = pd.read_sql_query(query, self.db_manager.connection)
            self.flightsData['date_lecture'] = datetime.now()
            self.logger.info(f"Données lues avec succès depuis la table {table_name}.")

            return self.flightsData

        except Exception as e:
            print(f"Une erreur s'est produite lors de la lecture depuis la base de données : {e}")
            self.logger.exception(f"Une erreur s'est produite lors de la lecture depuis la base de données : {e}")
            raise e

    def drop_columns(self, data, columns_to_drop):
        """Supprimer les colonnes spécifiées du DataFrame."""
        try:
            self.logger.info("Suppression des colonnes spécifiées du DataFrame.")

            # Assurez-vous que les colonnes à supprimer existent dans le DataFrame
            existing_columns = set(data.columns)
            columns_to_drop = set(columns_to_drop)
            invalid_columns = columns_to_drop - existing_columns

            if invalid_columns:
                raise ValueError(f"Les colonnes suivantes n'existent pas dans le DataFrame : {', '.join(invalid_columns)}")

            # Supprimer les colonnes spécifiées
            data = data.drop(columns=columns_to_drop)

            self.logger.info("Colonnes supprimées avec succès.")

            return data

        except Exception as e:
            print(f"Une erreur s'est produite lors de la suppression des colonnes : {e}")
            self.logger.exception(f"Une erreur s'est produite lors de la suppression des colonnes : {e}")
            raise e
        
    def rename_columns(self, data, column_mapping):
        """Renommer les colonnes du DataFrame."""
        try:
            self.logger.info("Renommage des colonnes du DataFrame.")

            # Utiliser la méthode rename pour renommer les colonnes
            data.rename(columns=column_mapping, inplace=True)

            self.logger.info("Renommage des colonnes terminé avec succès.")

            return data

        except Exception as e:
            print(f"Une erreur s'est produite lors du renommage des colonnes : {e}")
            self.logger.exception(f"Une erreur s'est produite lors du renommage des colonnes : {e}")
            raise e
    
    def replace_zeros_in_column(self, data, column_to_process, new_value):
        """Remplacer les zéros dans une colonne par une nouvelle valeur."""
        try:
            self.logger.info(f"Remplacement des zéros dans la colonne {column_to_process} par la nouvelle valeur {new_value}.")

            # Remplacer les zéros par la nouvelle valeur
            data[column_to_process] = data[column_to_process].replace(0, new_value)

            self.logger.info("Remplacement des zéros terminé avec succès.")

            return data

        except Exception as e:
            print(f"Une erreur s'est produite lors du remplacement des zéros dans la colonne : {e}")
            self.logger.exception(f"Une erreur s'est produite lors du remplacement des zéros dans la colonne : {e}")
            raise e

    def replace_nulls_in_column(self, data, column_to_process, new_value):
        """Remplacer les valeurs nulles dans une colonne par une nouvelle valeur."""
        try:

            self.logger.info(f"Remplacement des valeurs nulles dans la colonne {column_to_process} par la nouvelle valeur {new_value}.")
        
            self.logger.info(f"Nombre de valeurs nulles remplacées dans {column_to_process} : {data[column_to_process].isnull().sum()}")

            # Remplacer les valeurs nulles par la nouvelle valeur
            data[column_to_process] = data[column_to_process].fillna(new_value)

            self.logger.info("Remplacement des valeurs nulles terminé avec succès.")
            self.logger.info(f"Remplacement des valeurs nulles dans la colonne {column_to_process} par la nouvelle valeur {new_value}.")

            return data

        except Exception as e:
            print(f"Une erreur s'est produite lors du remplacement des valeurs nulles dans la colonne : {e}")
            self.logger.exception(f"Une erreur s'est produite lors du remplacement des valeurs nulles dans la colonne : {e}")
            raise e

    def drop_duplicates(self, data):
        try:
            self.logger.info("Suppression des lignes dupliquées dans le DataFrame.")
            new_data = data.drop_duplicates(inplace=True)
            self.logger.info("Lignes dupliquées supprimées avec succès.")
            return new_data
        
        except Exception as e:
            print(f"Une erreur s'est produite lors de la suppression des lignes dupliquées : {e}")
            self.logger.exception(f"Une erreur s'est produite lors de la suppression des lignes dupliquées : {e}")
            raise e

    def split_hour_indicator(self, data, hour_columns):
        """Séparer l'heure et l'indicateur AM/PM dans deux colonnes distinctes et supprimer les colonnes d'origine."""
        try:
            self.logger.info("Séparation de l'heure et de l'indicateur AM/PM.")

            for column in hour_columns:
                # Vérifier si la colonne existe dans le DataFrame
                if column in data.columns:
                    # Extraire l'heure et l'indicateur AM/PM
                    data[column + '_heure'] = data[column].apply(lambda x: x.split()[0] if isinstance(x, str) else x)
                    data[column + '_indicateur'] = data[column].apply(lambda x: x.split()[1] if isinstance(x, str) and len(x.split()) > 1 else '')
                    
                    # Supprimer la colonne d'origine
                    data = data.drop(columns=[column])
                    
                else:
                    self.logger.warning(f"La colonne {column} n'existe pas dans le DataFrame.")

            self.logger.info("Séparation de l'heure et de l'indicateur AM/PM terminée avec succès.")

            return data

        except Exception as e:
            print(f"Une erreur s'est produite lors de la séparation de l'heure et de l'indicateur AM/PM : {e}")
            self.logger.exception(f"Une erreur s'est produite lors de la séparation de l'heure et de l'indicateur AM/PM : {e}")
            raise e
        
    def fill_missing_prices(self, data):
        """Remplir les valeurs manquantes des prix en fonction de l'origine, de la destination et de la compagnie aérienne."""
        try:
            self.logger.info("Remplissage des valeurs manquantes des prix.")

            # Créer un masque pour les valeurs manquantes dans la colonne des prix
            missing_values_mask = data['Price'].isnull()

            # Parcourir chaque ligne et remplacer les valeurs manquantes dans la colonne des prix
            for index, row in data[missing_values_mask].iterrows():
                # Recherchez d'abord une ligne correspondante avec des valeurs non nulles pour l'origine, la destination et la compagnie aérienne
                matching_row = data[
                    (data['Origin'] == row['Origin']) &
                    (data['Destination'] == row['Destination']) &
                    (data['Airlines'] == row['Airlines']) &
                    (~data['Price'].isnull())
                ].head(1)

                # Si aucune ligne correspondante n'est trouvée, recherchez une ligne basée uniquement sur l'origine et la destination
                if matching_row.empty:
                    matching_row = data[
                        (data['Origin'] == row['Origin']) &
                        (data['Destination'] == row['Destination']) &
                        (~data['Price'].isnull())
                    ].head(1)

                # Si une ligne correspondante est trouvée, remplacer la valeur manquante
                if not matching_row.empty:
                    data.at[index, 'Price'] = matching_row.iloc[0]['Price']

            self.logger.info("Remplissage des valeurs manquantes des prix terminé avec succès.")

            return data

        except Exception as e:
            print(f"Une erreur générale s'est produite lors du remplissage des valeurs manquantes des prix : {e}")
            self.logger.exception(f"Une erreur générale s'est produite lors du remplissage des valeurs manquantes des prix : {e}")
            raise e
        
    def extract_currency_and_value(self, data, column_to_process):
        try:
            self.logger.info(f"Extraction de la devise et de la valeur depuis la colonne {column_to_process}.")

            # Utiliser une expression régulière pour extraire la valeur numérique et la devise
            pattern = r'(?P<currency>[A-Za-z]+)\s*(?P<value>[0-9,.]+)'
            extracted_data = data[column_to_process].str.extract(pattern)

            # Ajouter les colonnes extraites au DataFrame d'origine
            data[column_to_process + '_currency'] = extracted_data['currency']
            data[column_to_process + '_value'] = extracted_data['value']

            self.logger.info("Extraction de la devise et de la valeur terminée avec succès.")

            return data

        except Exception as e:
            print(f"Une erreur s'est produite lors de l'extraction de la devise et de la valeur : {e}")
            self.logger.exception(f"Une erreur s'est produite lors de l'extraction de la devise et de la valeur : {e}")
            raise e
        
    def extract_percentage_value(self, data, column_to_process):
        try:
            self.logger.info(f"Extraction de la valeur en pourcentage depuis la colonne {column_to_process}.")

            # Utiliser une expression régulière pour extraire la valeur en pourcentage
            pattern = r'([-+]?\d*\.\d+|\d+)%'
            data[column_to_process + '_percentage'] = data[column_to_process].str.extract(pattern)

            self.logger.info("Extraction de la valeur en pourcentage terminée avec succès.")

            return data

        except Exception as e:
            print(f"Une erreur s'est produite lors de l'extraction de la valeur en pourcentage : {e}")
            self.logger.exception(f"Une erreur s'est produite lors de l'extraction de la valeur en pourcentage : {e}")
            raise e
   
    def calculate_hours_from_time_columns(self, data, time_columns):
        try:
            self.logger.info(f"Calculating hours from the specified time columns: {time_columns}.")

            for time_column in time_columns:
                # Use regular expressions to extract hours and minutes from the time column
                pattern = r'(?:(\d+)\s*hr)?\s*(?:(\d+)\s*min)?'
                extracted_data = data[time_column].str.extract(pattern)

                # Convert extracted hours and minutes to timedelta and then to hours
                hours = pd.to_numeric(extracted_data[0], errors='coerce').fillna(0)
                minutes = pd.to_numeric(extracted_data[1], errors='coerce').fillna(0)
                total_hours = hours + minutes / 60

                # Add a new column with calculated hours
                data[f"{time_column}_hours"] = total_hours

                self.logger.info(f"Calculation of hours from {time_column} column completed successfully.")

            self.logger.info("Calculation of hours from all specified time columns completed successfully.")

            return data

        except Exception as e:
            print(f"An error occurred while calculating hours from specified time columns: {e}")
            self.logger.exception(f"An error occurred while calculating hours from specified time columns: {e}")
            raise e
    
    def process_flights_data(self):
        try:
            self.logger.info("Initialisation du traitement des données sur les vols.")
            
            # Lire les données depuis la base de données
            brute_flight_data = self.read_from_database("FlightsData")
            
            # Supprimer les lignes dupliquées
            self.drop_duplicates(brute_flight_data)
            
            # Renommer les colonnes
            columns_mapping = {'DepartTimeLeg1': 'DepartTime', 'ArrivalTimeLeg1': 'ArrivalTime'}
            renamed_columns_data = self.rename_columns(brute_flight_data, columns_mapping)
            
            # Séparer l'heure et l'indicateur AM/PM
            dates_processed_data = self.split_hour_indicator(renamed_columns_data, {"DepartTime", "ArrivalTime"})
            # Remplir les valeurs manquantes des prix
            
            filled_prices_data =  self.fill_missing_prices(dates_processed_data)

            finnna_data = self.replace_nulls_in_column(filled_prices_data , "LayoverTime" ,  "0")
            stop_location_filled_data = self.replace_nulls_in_column(finnna_data , "StopLocation" ,  "No Stops")
            final_data = self.replace_nulls_in_column(stop_location_filled_data , "TripType" ,  "Not Defined")

            data_with_currency = self.extract_currency_and_value(final_data, 'Price')
            data_with_percentage = self.extract_percentage_value(data_with_currency, 'EmissionAvgDiff')

            ff_data = self.drop_columns(data_with_percentage ,{"EmissionAvgDiff" , "Price"  , "date_lecture"} )
            
            time_columns_to_process = ['LayoverTime', 'TravelTime']
            data_with_calculated_hours = self.calculate_hours_from_time_columns(final_data, time_columns_to_process)

            df = pd.DataFrame(data_with_calculated_hours)
            df.to_csv("test.csv")

            self.logger.info("Traitement des données sur les vols terminé avec succès.")

            return df

        except Exception as e:
            self.logger.exception(f"Une erreur s'est produite lors du traitement des données sur les vols : {e}")
            raise e

