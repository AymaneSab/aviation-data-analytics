from opensky_api import OpenSkyApi
import csv

try:
    api = OpenSkyApi()
    arrivals = api.get_arrivals_by_airport("EDDF", 1517227200, 1517230800)

    if arrivals:
        with open('data_collection/collected_data/arrivals.csv', 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = arrivals[0].__dict__.keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            # Write header
            writer.writeheader()

            # Write data
            for flight in arrivals:
                writer.writerow(flight.__dict__)

except Exception as e:
    print(f"Une erreur s'est produite : {e}")
