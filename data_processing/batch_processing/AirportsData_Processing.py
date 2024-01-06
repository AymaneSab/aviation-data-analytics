import pandas as pd

# Charger le fichier CSV
airports_data = '/Users/sabri/Desktop/Study /Youcode/Github/aviation-data-analytics/data_collection/data_sources/airport_info.csv'  
df = pd.read_csv(airports_data)

# Créer une nouvelle DataFrame avec les colonnes spécifiées sans valeurs nulles ou vides
filtered_df = df[['name', 'latitude', 'longitude', 'city', 'country']]

# Utiliser dropna() pour supprimer les lignes avec des valeurs nulles
filtered_df = filtered_df.dropna()

# Utiliser drop_duplicates() pour supprimer les lignes en double
filtered_df = filtered_df.drop_duplicates()

# Afficher la nouvelle DataFrame
filtered_df.to_csv("data_collection/data_sources/airport_info.csv")
