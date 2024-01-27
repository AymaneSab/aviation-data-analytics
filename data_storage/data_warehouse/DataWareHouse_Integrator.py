import os
import logging
from datetime import datetime
import sys
import pandas as pd
from datetime import datetime


sys.path.append('/Users/sabri/Desktop/Study /Youcode/Github/aviation-data-analytics/data_collection/integrators/')
sys.path.append('/Users/sabri/Desktop/Study /Youcode/Github/aviation-data-analytics/data_processing/batch_processing')

from DataBase_Controller import DBManager
from Flights_DataProcessing import FlightsDataProcessor

class FlightsScrapper:

    def __init__(self):
        self.logger = self.setup_FlightsScrapper_logging("Log/FlightsScrapper", "FlightsScrapper.log")

    def setup_FlightsScrapper_logging(self, log_directory, logger_name):
        os.makedirs(log_directory, exist_ok=True)

        log_filename = logger_name
        log_filepath = os.path.join(log_directory, log_filename)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        handler = logging.FileHandler(log_filepath)
        handler.setLevel(logging.INFO)
        handler.setFormatter(formatter)

        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)

        return logger
    
    def extract_date_components(self , input_date):
        try:
            # Parse the input date string into a datetime object
            date_object = datetime.strptime(input_date, "%Y-%m-%d")

            # Extract year, month, and day components
            year = date_object.year
            month = date_object.month
            day = date_object.day

            return year, month, day

        except ValueError as e:
            print(f"Error parsing date: {e}")
            return None
    
    def create_fact_table(self, db_manager):
        try:
            # Define the columns for the fact table
            fact_table_name = "FactFlights"
            columns_definition = '''
                FactID INT IDENTITY(1,1) PRIMARY KEY,
                Price Varchar(255),
                Departure_Time Varchar(255) , 
                Arrival_Time Varchar(255) , 
                TravelTime Varchar(255),
                NumStops INT,
                LayoverTime Varchar(255),
                CO2Emission Varchar(255),
                StopLocationID Varchar(100),
                TripType INT FOREIGN KEY REFERENCES dbo.Trip(TripID),
                Departure_Time_Indicator INT FOREIGN KEY REFERENCES dbo.Time(TimeID),
                Arrival_Time_Idicator INT FOREIGN KEY REFERENCES dbo.Time(TimeID), 
                PriceCurrency  INT FOREIGN KEY REFERENCES dbo.Price(PriceID) ,
                LeaveDateID INT FOREIGN KEY REFERENCES dbo.Date(DateID),
                ReturnDateID INT FOREIGN KEY REFERENCES dbo.Date(DateID),
                AirlinesID INT FOREIGN KEY REFERENCES dbo.Airlines(AirlinesID),
                DepartureLocationID INT FOREIGN KEY REFERENCES dbo.Location(LocationID),
                ArrivalLocationID INT FOREIGN KEY REFERENCES dbo.Location(LocationID),
                AccessDateID INT FOREIGN KEY REFERENCES dbo.Date(DateID)
            '''

            db_manager.create_table_if_not_exists(fact_table_name, columns_definition)
            self.logger.info(f"Fact table '{fact_table_name}' created successfully")

        except Exception as e:
            self.logger.error(f"Error creating fact table: {e}")

    def create_dimension_tables(self, db_manager):
        try:
            # Define the columns for each dimension table
            dimension_tables = {
                "Location": "LocationID INT IDENTITY(1,1) PRIMARY KEY, Country VARCHAR(255), City VARCHAR(255)",
                "Airlines": "AirlinesID INT IDENTITY(1,1) PRIMARY KEY, Name VARCHAR(255) ",
                "Time": "TimeID INT IDENTITY(1,1) PRIMARY KEY, Indicator VARCHAR(255)",
                "Price": "PriceID INT IDENTITY(1,1) PRIMARY KEY, Currency VARCHAR(255)",
                "Date": "DateID INT IDENTITY(1,1) PRIMARY KEY, Year VARCHAR(255) , Month VARCHAR(255) , Day VARCHAR(255) ",
                "Trip": "TripID INT IDENTITY(1,1) PRIMARY KEY, TripType VARCHAR(255) "
            }

            for table_name, columns_definition in dimension_tables.items():
                db_manager.create_table_if_not_exists(table_name, columns_definition)
                self.logger.info(f"Dimension table '{table_name}' created successfully")

        except Exception as e:
            self.logger.error(f"Error creating dimension tables: {e}")
        
    def check_dimension_existence(self, db_manager, table_name, columns, values):
        try:
            # Check if the dimension data already exists
            if isinstance(columns, str):
                # If a single column is provided, convert it to a list for consistency
                columns = [columns]
                values = [values]

            conditions = ' AND '.join([f'{table_name}.{column} = ?' for column in columns])
            query = f"SELECT {table_name}ID FROM {table_name} WHERE {conditions};"

            self.logger.info(f"query: {query}")

            result = db_manager.cursor.execute(query, values).fetchone()

            if result:
                # If the data exists, return the existing dimension ID
                return result[0]
            else:
                # If the data doesn't exist, insert the new dimension data
                columns_definition = ', '.join(columns)
                placeholders = ', '.join(['?' for _ in values])
                insert_query = f"INSERT INTO {table_name} ({columns_definition}) VALUES ({placeholders});"
                self.logger.info(f"insert_query: {insert_query}")

                db_manager.cursor.execute(insert_query, values)

                # Retrieve the last inserted ID using SCOPE_IDENTITY()
                query_last_id = f"SELECT SCOPE_IDENTITY() AS LastID;"
                new_id = db_manager.cursor.execute(query_last_id).fetchone()[0]
                db_manager.connection.commit()

                self.logger.info(f"Data inserted successfully into {table_name} table")

                return new_id

        except Exception as e:
            self.logger.error(f"Error checking dimension existence or inserting data into {table_name} table: {e}")
            return None
    
    def insert_processed_data(self, db_manager):
        try:
            self.logger.info("Inserting processed data into data warehouse.")

            processor = FlightsDataProcessor('10.211.55.3', 'Flights_StagingArea_DB', 'Database_Administrator', 'AllahSave.1234/')
            processed_data = processor.process_flights_data() 

            self.logger.info("Data Processed Succefully ")

            # Check existence in dimension tables and insert into the FactFlights table
            for index  , row in processed_data.iterrows():
                airlines_id = self.check_dimension_existence(db_manager, 'Airlines',  'Name' , row['Airlines'])
                self.logger.info(f"airlines_id : {airlines_id}")

                departure_time_indicator_id = self.check_dimension_existence(db_manager, 'Time', 'Indicator' ,  row['DepartTime_indicateur'])
                arrival_time_indicator_id = self.check_dimension_existence(db_manager, 'Time', 'Indicator' , row['ArrivalTime_indicateur'])

                self.logger.info(f"departure_time_indicator_id : {departure_time_indicator_id}")
                self.logger.info(f"arrival_time_indicator_id : {arrival_time_indicator_id}")

                price_currency_id = self.check_dimension_existence(db_manager, 'Price',"Currency",  row['Price_currency'])
                self.logger.info(f"price_currency_id : {price_currency_id}")

                TripTypeID = self.check_dimension_existence(db_manager, 'Trip', "TripType", row['TripType'])
                self.logger.info(f"TripTypeID : {TripTypeID}")

                departure_location_id = self.check_dimension_existence(db_manager, 'Location',['Country' , 'City'], [row['Origin_Country'], row['Origin_City']])
                arrival_location_id = self.check_dimension_existence(db_manager, 'Location', ['Country' , 'City'] , [row['Destination_Country'], row['Destination_City']])

                self.logger.info(f"departure_location_id : {departure_location_id}")
                self.logger.info(f"arrival_location_id : {arrival_location_id}")

                LeaveDate_Year , LeaveDate_Month , LeaveDate_Day  = self.extract_date_components(row['LeaveDate'])
                DepartureDate_Year , DepartureDate_Month , DepartureDate_Day   =  self.extract_date_components(row['ReturnDate'])
                AcccessDate_Year , AcccessDate_Month, AcccessDate_Day  = self.extract_date_components(row['AccessDate'])

                LeaveDateID =  self.check_dimension_existence(db_manager, 'Date' ,  ['Year' , 'Month' , 'Day']  , [LeaveDate_Year , LeaveDate_Month , LeaveDate_Day ])
                ReturnDateID =  self.check_dimension_existence(db_manager,'Date' ,['Year' , 'Month' , 'Day'], [DepartureDate_Year , DepartureDate_Month , DepartureDate_Day] )
                AccessDateID =  self.check_dimension_existence(db_manager, 'Date' ,['Year' , 'Month' , 'Day'] , [AcccessDate_Year , AcccessDate_Month, AcccessDate_Day] )

                self.logger.info(f"LeaveDateID :  {LeaveDateID}")
                self.logger.info(f"ReturnDateID : {ReturnDateID}")
                self.logger.info(f"AccessDateID : {AccessDateID}")


                fact_data = (row['Price_value'], row['DepartTime_heure'], row["ArrivalTime_heure"] , row["TravelTime_hours"], row["NumStops"] , row["LayoverTime_hours"]  , row["CO2Emission"]  , row["StopLocation"] ,
                            TripTypeID , departure_time_indicator_id , arrival_time_indicator_id , price_currency_id , 
                            LeaveDateID, ReturnDateID , airlines_id , departure_location_id , arrival_location_id ,  AccessDateID
                            )

                # Check if any element in fact_data is empty or None
                if all(value is not None and value != '' for value in fact_data):
                    db_manager.insert_data("FactFlights", fact_data)
                    self.logger.info("Processed data inserted successfully into FactFlights table")
                else:
                    self.logger.error(f"Data skipped due to missing information in fact_data: {fact_data}")

            else:
                self.logger.error(f"Data skipped due to missing dimension information: {row}")

        except Exception as e:
            self.logger.error(f"Error inserting processed data: {e}")


if __name__ == "__main__":

    # Create an instance of DBManager and connect to the database
    db_manager = DBManager('10.211.55.3', 'Flights_DataWareHouse', 'Database_Administrator', 'AllahSave.1234/')
    db_manager.connect()

    # Create an instance of FlightsScrapper and create fact and dimension tables
    flights_scrapper = FlightsScrapper()
    flights_scrapper.create_dimension_tables(db_manager)
    flights_scrapper.create_fact_table(db_manager)

    # Insert sample data into dimension and fact tables
    flights_scrapper.insert_processed_data(db_manager)

    # Close the database connection
    db_manager.close()
