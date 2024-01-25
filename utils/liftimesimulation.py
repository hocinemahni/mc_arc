import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


def parse_trace_files(file_paths, ignore_head=True):
    file_ids_metadata = {}
    all_operations = []

    for path in file_paths:
        with open(path, 'r') as f:
            for line in f:
                split_line = line.strip().split()
                if len(split_line) < 3:
                    continue  # Ignorer les lignes mal formatées
                timestamp, op_code_full, uid = int(split_line[0]) / 1000, split_line[1], split_line[2]
                size = int(split_line[3]) if len(split_line) > 3 else 0
                offset_start = int(split_line[4]) if len(split_line) > 4 else 0
                offset_end = int(split_line[5]) if len(split_line) > 5 else size

                op_code = op_code_full.split('.')[1]

                if ignore_head and op_code == "HEAD":
                    continue

                # Exclure les opérations COPY et DELETE
                if op_code in ["COPY", "DELETE"]:
                    continue

                # Mise à jour des métadonnées pour les autres opérations
                if uid not in file_ids_metadata:
                    file_ids_metadata[uid] = [timestamp, timestamp, size]  # [creation_time, last_access_time, size]
                else:
                    file_ids_metadata[uid][1] = timestamp  # Update last access time

                all_operations.append((timestamp, op_code, uid, offset_start, offset_end))

    # Calculating the lifetime for each file
    lifetime_per_fileid = {uid: (last_access - creation_time, size) for uid, (creation_time, last_access, size) in file_ids_metadata.items()}

    return all_operations, lifetime_per_fileid

def save_data_to_csv(data, file_name, column_names):
    df = pd.DataFrame(data, columns=column_names)
    df.to_csv(file_name, index=False)

def save_lifetimes_figure(lifetime_per_fileid, output_directory):
    # Conversion des durées de vie en une liste pour le graphique
    lifetimes = [lifetime for lifetime, size in lifetime_per_fileid.values()]

    # Tri des durées de vie pour le graphique
    sorted_lifetimes = sorted(lifetimes)

    # Utilisation d'une échelle logarithmique pour les durées de vie
    log_lifetimes = [np.log10(lifetime + 1) for lifetime in sorted_lifetimes]  # +1 pour éviter log(0)

    # Calcul de la proportion cumulée
    y_values = np.arange(1, len(log_lifetimes) + 1) / len(log_lifetimes)

    # Création du graphique
    plt.figure(figsize=(10, 6))
    plt.plot(log_lifetimes, y_values, marker='.', linestyle='-')

    # Définition des titres et labels
    plt.title('Distribution Cumulative des Durées de Vie des Fichiers')
    plt.xlabel('Durée de Vie (log10 secondes)')
    plt.ylabel('Proportion Cumulative des Fichiers')

    # Ajout de la grille
    plt.grid(True)

    # Enregistrement du graphique
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    plt.savefig(os.path.join(output_directory_images, 'lifetime_distribution.png'))
    plt.close()

# Exemple d'utilisation
file_paths = ['IBMObjectStoreTrace009Part0.csv']
output_directory = 'data3/'
output_directory_images = 'output/'
all_operations, lifetime_per_fileid = parse_trace_files(file_paths)
save_data_to_csv(all_operations, 'data3/IBMObjectStoreTrace009Part0_request.csv', ['timestamp', 'requestType', 'filename', 'offsetStart', 'offsetEnd'])
save_data_to_csv([(uid, size, lifetime) for uid, (lifetime, size) in lifetime_per_fileid.items()], 'data3/IBMObjectStoreTrace009Part0_metadata.csv', ['filename', 'size', 'lifetime'])
save_lifetimes_figure(lifetime_per_fileid, output_directory)
