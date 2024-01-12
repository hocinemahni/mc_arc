import csv

# Chemin vers votre fichier CSV
file_path = 'data3/IBMObjectStoreTrace007Part0_request.csv'

# Initialisation de la variable pour stocker la somme totale
total_size = 0

# Lecture du fichier CSV
with open(file_path, mode='r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        # Calcul de la taille pour chaque ligne et ajout à la somme totale
        offset_start = int(row['offsetStart'])
        offset_end = int(row['offsetEnd'])
        total_size += offset_end - offset_start

print(f"La taille totale du dataset est: {total_size}")
