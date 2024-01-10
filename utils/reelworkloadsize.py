import csv

# Chemin vers le fichier CSV du workload
file_path = 'data3/IBMObjectStoreTrace001Part0_request.csv'

# Initialisation de la variable pour stocker la somme totale
total_size = 0

# Ensemble pour suivre les lignes uniques
unique_lines = set()

# Lecture du fichier CSV
with open(file_path, mode='r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        # Ignorer les requêtes de type "PUT"
        if row['requestType'] == 'PUT':
            continue

        # Créer une clé unique pour chaque ligne
        unique_key = (row['filename'], row['offsetStart'], row['offsetEnd'])

        # Vérifier si la clé est déjà dans l'ensemble
        if unique_key not in unique_lines:
            unique_lines.add(unique_key)  # Ajouter la clé unique à l'ensemble

            # Calcul de la taille pour chaque ligne unique et ajout à la somme totale
            offset_start = int(row['offsetStart'])
            offset_end = int(row['offsetEnd'])
            total_size += offset_end - offset_start

print(f"La taille totale du dataset est: {total_size}")
