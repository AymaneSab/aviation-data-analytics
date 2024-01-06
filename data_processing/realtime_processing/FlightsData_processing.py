import pandas as pd

# Charger le fichier CSV
airports_data = '/Users/sabri/Desktop/Study /Youcode/Github/aviation-data-analytics/airport_info.csv'  
df = pd.read_csv(airports_data)

# Afficher la table descriptive
table_descriptive = df.describe(include='all')

# Afficher le pourcentage de valeurs manquantes par colonne
pourcentage_valeurs_manquantes = (df.isnull().mean() * 100).round(2)

# Afficher les types de données
types_de_donnees = df.dtypes

# Créer une nouvelle DataFrame pour le pourcentage de valeurs manquantes et les types de données
info_df = pd.DataFrame({'Pourcentage de valeurs manquantes': pourcentage_valeurs_manquantes, 'Type de données': types_de_donnees})

# Afficher la table descriptive avec les informations demandées
table_descriptive_with_missing = pd.concat([table_descriptive, info_df.T], axis=1)

# Afficher la table descriptive avec les informations demandées
print(table_descriptive_with_missing)