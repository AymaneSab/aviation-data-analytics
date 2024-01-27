import os
import logging
import atexit
import pyodbc
from datetime  import  datetime


class DBManager:

    def __init__(self, server, database, username, password):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.connection = None
        self.cursor = None
        self.logger = self.setup_dbmanager_logging("Log/DBManager", "DBManager.log")

    def setup_dbmanager_logging(self ,log_directory, logger_name):
       
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

    def log_info(self, message):
        self.logger.info(message)

    def log_error(self, message):
        self.logger.error(message)

    def connect(self):
        try:
            # Connexion à la base de données SQL Server
            self.connection = pyodbc.connect(f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password}')
            self.cursor = self.connection.cursor()
            self.log_info("Connexion réussie à la base de données")

        except Exception as e:
            self.log_error(f"Erreur lors de la connexion à la base de données : {e}")

    def close(self):
        try:
            # Fermeture du curseur et de la connexion à la base de données
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            self.log_info("Fermeture de la connexion à la base de données.")
        except Exception as e:
            self.log_error(f"Erreur lors de la fermeture de la connexion à la base de données : {e}")

    def create_table_if_not_exists(self, table_name, columns_definition):
        try:
            # Création de la table si elle n'existe pas déjà
            self.cursor.execute(f'''
                IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}')
                BEGIN
                    CREATE TABLE {table_name} (
                        {columns_definition}
                    );
                END
            ''')
            self.connection.commit()
        except Exception as e:
            self.log_error(f"Erreur lors de la création de la table '{table_name}' : {e}")
            raise  # Renvoyer l'exception pour signaler l'erreur à l'appelant

    def insert_data(self, table_name, values):
        try:
            placeholders = ', '.join(['?' for _ in values])
            query = f"INSERT INTO {table_name} VALUES ({placeholders});"
            self.cursor.execute(query, values)
            self.connection.commit()
            
            self.log_info(f"Data Inserted Succefully into table : {table_name}")

        except Exception as e:
            self.log_error(f"Error inserting data into table '{table_name}': {e}")
            pass
