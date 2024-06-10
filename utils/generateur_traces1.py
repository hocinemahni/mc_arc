import random
import numpy as np


def generate_sequential_ops(start_offset, max_size, block_size, is_sequential):
    if is_sequential:
        end_offset = min(start_offset + block_size, max_size)
    else:
        end_offset = random.randint(start_offset, min(start_offset + block_size, max_size))
    blocks_accessed = (end_offset - start_offset) // block_size if start_offset != end_offset else 1
    return end_offset, blocks_accessed


def generate_io_trace(num_files, num_operations, block_size, seq_percent, output_file_io, output_file_meta,
        num_files_with_zero_lifetime, hot_files_percent):
    file_metadata = {}
    operations_count = {'read': 0, 'write': 0, 'create': 0}
    file_access_counts = {}
    file_blocks_accessed = {}

    total_addressable_space = 0
    total_file_size = 0
    millis_counter = 0

    hot_files_count = int(num_files * (hot_files_percent / 100.0))
    hot_files = random.sample(range(1, num_files + 1), k=hot_files_count)

    with open(output_file_io, 'w') as io_f, open(output_file_meta, 'w') as meta_f:
        io_f.write("timestamp,requestType,filename,offsetStart,offsetEnd\n")
        meta_f.write("filename,size,firstAccessTime,lastAccessTime,lifetime\n")

        for _ in range(num_operations):
            millis_counter += random.randint(100, 500)
            if hot_files and random.random() < 0.7:
                file_id = f"File_{random.choice(hot_files)}"
            else:
                file_id = f"File_{random.randint(1, num_files)}"
            is_sequential = random.random() < seq_percent / 100.0
            operation = 'read' if random.random() < 0.7 else 'write'

            if file_id not in file_metadata:
                size = int(np.random.lognormal(mean=13, sigma=0.5))
                file_metadata[file_id] = {'size': size, 'firstAccessTime': millis_counter, 'lastAccessTime': millis_counter}
                total_file_size += size
                file_access_counts[file_id] = 1
                file_blocks_accessed[file_id] = 0
                io_f.write(f"{millis_counter},create,{file_id},0,{size}\n")
                operations_count['create'] += 1
            else:
                file_metadata[file_id]['lastAccessTime'] = millis_counter
                file_access_counts[file_id] += 1
                operations_count[operation] += 1

            if operation != 'create':
                size = file_metadata[file_id]['size']
                start_offset = random.randint(0, size - 1)
                end_offset, blocks_accessed = generate_sequential_ops(start_offset, size, block_size, is_sequential)
                file_blocks_accessed[file_id] += blocks_accessed
                total_addressable_space += end_offset - start_offset
                io_f.write(f"{millis_counter},{operation},{file_id},{start_offset},{end_offset}\n")

        for file_id, metadata in file_metadata.items():
            lifetime = metadata['lastAccessTime'] - metadata['firstAccessTime']
            meta_f.write(f"{file_id},{metadata['size']},{metadata['firstAccessTime']},{metadata['lastAccessTime']},{lifetime}\n")

    print("Détails des accès par fichier :")
    for file_id, repetitions in file_access_counts.items():
        print(f"{file_id} a été accédé {repetitions} fois, avec {file_blocks_accessed[file_id]} blocs accédés.")

    print(f"Espace adressable du workload: {total_addressable_space} octets")
    print(f"Taille totale de tous les fichiers: {total_file_size} octets")
    print("Répartition des opérations d'I/O :")
    for operation, count in operations_count.items():
        print(f"  {operation}: {count}")

    print(f"Nombre total de fichiers: {num_files}")
    print(f"Pourcentage de fichiers 'chauds': {hot_files_percent}%")
    print(f"Nombre de fichiers 'chauds': {hot_files_count}")
    print("Génération des traces d'I/O pour un environnement HPC simulé terminée.")


# Paramètres de simulation
num_files = 100 # Nombre de fichiers à créer
num_operations = 1000 # Nombre d'opérations d'I/O à générer
block_size = 1024  # Blocs de 1 ko pour simuler les accès en HPC
seq_percent = 50  # 95% de probabilité pour les accès séquentiels
hot_files_percent = 5  # 20% des fichiers sont considérés comme "chauds"
output_file_io = "data3/io_trace_hpc.csv" # Fichier de sortie pour les opérations d'I/O
output_file_meta = "data3/file_metadata_hpc.csv"  # Fichier de sortie pour les métadonnées des fichiers
num_files_with_zero_lifetime = 30  # Nombre de fichiers à créer avec un lifetime de 0

generate_io_trace(num_files, num_operations, block_size, seq_percent, output_file_io, output_file_meta, num_files_with_zero_lifetime, hot_files_percent)
# Path: generateur_traces.py (fin)