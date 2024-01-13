
import os
import logging
import datetime
import atexit
import pyodbc

class DBManager:

    def __init__(self, server, database, username, password):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.connection = None
        self.cursor = None
        self.logger = self.setup_logging()

    def setup_logging(self):
        log_directory = "Log/DataBase_Controller"
        os.makedirs(log_directory, exist_ok=True)
        log_filename = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S.log")
        log_filepath = os.path.join(log_directory, log_filename)

        logging.basicConfig(filename=log_filepath, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

        logger = logging.getLogger(__name__)
        atexit.register(logging.shutdown)  # Ensure proper shutdown of the logger

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
            self.log_info("Connexion réussie à la base de données.")
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
            self.log_info(f"Table '{table_name}' créée avec succès.")
        except Exception as e:
            self.log_error(f"Erreur lors de la création de la table '{table_name}' : {e}")
            raise  # Renvoyer l'exception pour signaler l'erreur à l'appelant

    def insert_data(self, table_name, values):
        try:
            # Inserting data into the specified table
            self.cursor.execute(f"INSERT INTO {table_name} VALUES {values}")
            self.connection.commit()
            self.log_info(f"Data inserted into the table '{table_name}' successfully.")
        except Exception as e:
            self.log_error(f"Error inserting data into table '{table_name}': {e}")
            raise  # Raise the exception to signal the error to the caller
