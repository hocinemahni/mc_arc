import pandas as pd

#  le chemin du fichier CSV
csv_file_path = 'data3/IBMObjectStoreTrace000Part0_request.csv'

# Lecture du fichier CSV
df = pd.read_csv(csv_file_path)

# Calcul de la somme des valeurs dans la colonne 'io_size'
total_io_size = df['io_size'].sum()

print("La somme totale de 'io_size' est :", total_io_size)
