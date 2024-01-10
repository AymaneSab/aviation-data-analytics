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
        log_directory = "Log/AirPorts_DataCollection"
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
            self.connection = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password}')
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

# Exemple d'utilisation de la classe DBManager
if __name__ == "__main__":
    # Remplacez ces valeurs par les informations de votre base de données
    server = 'your_server'
    database = 'your_database'
    username = 'your_username'
    password = 'your_password'

    # Création d'une instance de DBManager
    db_manager = DBManager(server, database, username, password)

    # Connexion à la base de données
    db_manager.connect()

    # Exemple de création de table
    table_name = 'Departures'
    columns_definition = '''
        icao24 NVARCHAR(255),
        callsign NVARCHAR(255),
        origin_country NVARCHAR(255),
        time_position INT,
        last_contact INT,
        longitude FLOAT,
        latitude FLOAT,
        altitude FLOAT,
        on_ground BIT,
        velocity FLOAT,
        heading FLOAT,
        vertical_rate FLOAT,
        sensors INT,
        baro_altitude FLOAT,
        squawk NVARCHAR(255),
        spi BIT,
        position_source INT
    '''
    db_manager.create_table_if_not_exists(table_name, columns_definition)

    # Fermeture de la connexion à la base de données
    db_manager.close()
