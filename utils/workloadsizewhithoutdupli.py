import pandas as pd

# le chemin du fichier CSV
csv_file_path = 'data3/IBMObjectStoreTrace000Part0_request.csv'

# Lecture du fichier CSV
df = pd.read_csv(csv_file_path)

# Suppression des requêtes identiques (lignes en double)
df_unique = df.drop_duplicates()

# Calcul de la somme des valeurs dans la colonne 'io_size' après suppression des doublons
total_io_size = df_unique['io_size'].sum()

print("La somme totale de 'io_size' après suppression des requêtes identiques est :", total_io_size)
