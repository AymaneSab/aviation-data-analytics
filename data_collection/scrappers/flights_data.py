from time import sleep
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from datetime import date, datetime
from selenium.common.exceptions import WebDriverException
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import numpy as np
import pandas as pd
import os
import sys
import logging
import atexit
import signal


class _Scrape:

    def __init__(self):
        self._origin = None
        self._dest = None
        self._date_leave = None
        self._date_return = None
        self._data = pd.DataFrame()  # Set a default empty DataFrame
        self.logger = self.setup_logging()

        signal.signal(signal.SIGINT, self._save_data_on_exit)

    def setup_logging(self):
        log_directory = "Log/Flights_DataCollection"
        os.makedirs(log_directory, exist_ok=True)
        log_filepath = os.path.join(log_directory, "Logs.log")

        logging.basicConfig(filename=log_filepath, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

        logger = logging.getLogger(__name__)
        atexit.register(logging.shutdown) 

        return logger

    def __call__(self, *args):
        try:
            if len(args) == 4:
                self._set_properties(*args)
                self._data = self._scrape_data()
            else:
                self._set_properties(*(args[:-1]))
                self._data = args[-1]

            obj = self.clone()
            obj.data = self._data
            return obj

        except KeyboardInterrupt:
            self.logger.warning("Keyboard interruption. Saving scraped data.")
            self._save_data_on_exit()
            sys.exit(0)

    def __str__(self):
        return "{dl}: {org} --> {dest}\n{dr}: {dest} --> {org}".format(
            dl=self._date_leave,
            dr=self._date_return,
            org=self._origin,
            dest=self._dest
        )

    def __repr__(self):
        return "RESULTS FOR:\n{dl}: {org} --> {dest}\n{dr}: {dest} --> {org}".format(
            dl=self._date_leave,
            dr=self._date_return,
            org=self._origin,
            dest=self._dest
        )

    def clone(self):
        obj = _Scrape()
        obj._set_properties(
            self._origin, self._dest, self._date_leave, self._date_return
        )
        return obj

    '''
        scraper called - Setters .
    '''

    def _set_properties(self, *args):
        (
            self._origin, self._dest, self._date_leave, self._date_return
        ) = args

    @property
    def origin(self):
        return self._origin

    @origin.setter
    def origin(self, x: str) -> None:
        self._origin = x

    @property
    def dest(self):
        return self._dest

    @dest.setter
    def dest(self, x: str) -> None:
        self._dest = x

    @property
    def date_leave(self):
        return self._date_leave

    @date_leave.setter
    def date_leave(self, x: str) -> None:
        self._date_leave = x

    @property
    def date_return(self):
        return self._date_return

    @date_return.setter
    def date_return(self, x: str) -> None:
        self._date_return = x

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, x):
        self._data = x

    '''
        scraper operations - Functions .
    '''

    def _save_data_on_exit(self):
        if self._data is not None:
            csv_directory = "data_collection/collected_data/flights"
            os.makedirs(csv_directory, exist_ok=True) 

            csv_filepath = csv_directory + "/flights.csv"
            # Ajouter le paramètre mode='a' pour append
            self._data.to_csv(csv_filepath, index=False, mode='a', header=not os.path.exists(csv_filepath))
            self.logger.info(f"Données ajoutées au fichier CSV : {csv_filepath}")

    def _scrape_data(self):
        try:
            start_time = datetime.now()
            self.logger.info("Start Scrapping Data at {}".format(start_time) )

            url = self._make_url()
            if url:
                self._data = self._get_results(url)
                self.logger.info("Data Retrieved Succefully ")
                self._save_data_on_exit()

        except KeyboardInterrupt:
            self.logger.info("Programme interrompu par l'utilisateur .... Saving the current data ")
            raise
        except TimeoutException as te:
            self.logger.error(f"Un délai d'attente s'est produit pendant la demande d'URL dans _make_url_request - TimeOutException : {te}")
            pass
        except Exception as e:
            self.logger.error(f"An error occurred during data scraping: {e}")
            raise
        finally:
            end_time = datetime.now()
            self._save_data_on_exit()
            self.logger.info("End Scrapping Data at {}. Elapsed Time: {}".format(end_time, end_time - start_time))

    def _make_url(self):
        try:
            return 'https://www.google.com/travel/flights?q=Flights%20to%20{dest}%20from%20{org}%20on%20{dl}%20through%20{dr}'.format(
                dest=self._dest,        # Destination 
                org=self._origin,       # Origin
                dl=self._date_leave,    # date leave
                dr=self._date_return    # date return 
            )
        
        except Exception as e:
            self.logger.error(f"An error occurred while generating URL : {e}")
            raise

    def _get_results(self, url):
        try:
            results = _Scrape._make_url_request(self ,url)

            if results:
                flight_info = _Scrape._get_info(self , results)
                partition = _Scrape._partition_info(flight_info)
                flights_data = _Scrape._parse_columns(self , partition, self._date_leave, self._date_return)

                return flights_data 
            
        except Exception as e:
            self.logger.error(f"An error occurred while getting results: {e}")
            raise

        return None
    
    @staticmethod
    def _get_driver(self):
        try:
            chromedriver_path = "/Users/sabri/Desktop/Study /Youcode/Github/aviation-data-analytics/data_collection/scrappers/bin/chromedriver"
            chrom_binary_path = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"

            # Configure Chrome options with Opera binary location
            chrome_options = webdriver.ChromeOptions()
            chrome_options.binary_location = chrom_binary_path

            # Create a new instance of the Chrome driver with specified options and executable path
            driver = webdriver.Chrome(executable_path=chromedriver_path, options=chrome_options)

            return driver
        
        except WebDriverException as e:
            self.logger.error(f"Une erreur s'est produite dans _get_driver : {e}")
            raise

        except Exception as e:
            self.logger.error(f"An unexpected error occurred in _get_driver: {e}")
            raise

    @staticmethod
    def _make_url_request(self , url):
        try:
            driver = _Scrape._get_driver(self)
            driver.get(url)

            # Waiting and initial XPATH cleaning
            WebDriverWait(driver, timeout=60).until(lambda d: len(_Scrape._get_flight_elements(self,d)) > 100)

            results = _Scrape._get_flight_elements(self,driver)

            return results
        
        except TimeoutException as te:
            self.logger.error(f"Timeout occurred during URL request in _make_url_request - TimeOutException: {te}")
            raise
        except Exception as e:
            self.logger.error(f"An error occurred during URL request in _make_url_request: {e}")
            raise
        finally:
            if 'driver' in locals():
                driver.quit()

    @staticmethod
    def _get_flight_elements(self ,driver):
        try:
            flight_element = driver.find_element(by=By.XPATH, value='//body[@id = "yDmH0d"]').text.split('\n')
            return flight_element

        except NoSuchElementException as nse:
            self.logger.error(f"Erreur dans _get_flight_elements : Élément non trouvé. {nse}")
            # Propager l'exception pour une gestion ultérieure si nécessaire
            raise

        except Exception as e:
            self.logger.error(f"Une erreur s'est produite dans _get_flight_elements : {e}")
            # Propager l'exception pour une gestion ultérieure si nécessaire
            raise

        # Si aucune exception n'est levée, retourner une liste vide
        return []

    @staticmethod
    def _get_info(self,result):
        try:
            info = []
            collect = False

            for r in result:
                if 'more flights' in r:
                    collect = False

                if collect and 'price' not in r.lower() and 'prices' not in r.lower() and 'other' not in r.lower() and ' – ' not in r.lower():
                    info += [r]

                if r == 'Sort by:':
                    collect = True

            return info

        except Exception as e:
            self.logger.error(f"Une erreur s'est produite dans _get_info : {e}")
            # Propager l'exception pour une gestion ultérieure si nécessaire
            raise

    @staticmethod
    def _partition_info(info):
        i, grouped = 0, []
        while i < len(info) - 1:
            j = i + 2
            end = -1
            while j < len(info):
                if _Scrape._end_condition(info[j]):
                    end = j
                    break
                j += 1

            if end == -1:
                break
            grouped += [info[i:end]]
            i = end

        return grouped

    @staticmethod
    def _end_condition(x):
        if len(x) < 2:
            return False

        if x[-2] == "+":
            x = x[:-2]

        if x[-2:] == 'AM' or x[-2:] == 'PM':
            return True
        return False

    @staticmethod
    def _parse_columns(self, grouped, date_leave, date_return):
        flight_data = []  # List to store dictionaries representing each flight

        try:
            for g in grouped:
                if len(g) >= 9:
                    i_diff = 0

                    depart_time = g[0]
                    arrival_time = g[1]
                    i_diff += 1 if 'Separate tickets booked together' in g[2] else 0

                    airline = g[2 + i_diff]
                    travel_time = g[3 + i_diff]
                    origin = g[4 + i_diff].split('–')[0]
                    dest = g[4 + i_diff].split('–')[1]

                    num_stops = 0 if 'Nonstop' in g[5 + i_diff] else int(g[5 + i_diff].split('stop')[0])
                    stops = num_stops

                    stop_time = None if num_stops == 0 else (g[6 + i_diff].split('min')[0] if num_stops == 1 else None)
                    stop_location = None if num_stops == 0 else (
                        g[6 + i_diff].split('min')[1] if num_stops == 1 and 'min' in g[6 + i_diff] else [
                            g[6 + i_diff].split('hr')[1] if 'hr' in g[6 + i_diff] and num_stops == 1 else g[6 + i_diff]])

                    i_diff += 0 if num_stops == 0 else 1

                    price_str = g[8 + i_diff][3:] if len(g) > 8 + i_diff else ''
                    price_value = ''.join(char for char in price_str if char.isdigit() or char == '.')

                    flight_data.append({
                        'Leave Date': date_leave,
                        'Return Date': date_return,
                        'Depart Time (Leg 1)': depart_time,
                        'Arrival Time (Leg 1)': arrival_time,
                        'Airline(s)': airline,
                        'Travel Time': travel_time,
                        'Origin': origin,
                        'Destination': dest,
                        'Num Stops': stops,
                        'Layover Time': stop_time,
                        'Stop Location': stop_location,
                        'CO2 Emission': None,  # Fill in the appropriate value
                        'Emission Avg Diff (%)': None,  # Fill in the appropriate value
                        'Price ($)': price_value,
                        'Trip Type': g[9 + i_diff] if len(g) > 9 + i_diff else '',
                        'Access Date': date.today().strftime('%Y-%m-%d')
                    })

        except (ValueError, IndexError) as e:
            logging.error(f"An error occurred in _parse_columns: {e}")

        df = pd.DataFrame(flight_data)
        return df



