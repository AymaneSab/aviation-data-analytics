from opensky_api import OpenSkyApi
import csv

try:
    api = OpenSkyApi()
    departures = api.get_departures_by_airport("EDDF", 1517227200, 1517230800)

    if departures:
        with open('data_collection/collected_data/departures.csv', 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = departures[0].__dict__.keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            # Write header
            writer.writeheader()

            # Write data
            for flight in departures:
                writer.writerow(flight.__dict__)

except Exception as e:
    print(f"Une erreur s'est produite : {e}")
