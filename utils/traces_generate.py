import random
import numpy as np

def generate_sequential_ops(start_offset, max_size, block_size, is_sequential):
    """Génère un offset de fin, favorisant des accès séquentiels pour les workloads HPC."""
    if is_sequential:
        return min(start_offset + block_size, max_size)
    else:
        return random.randint(start_offset, min(start_offset + block_size, max_size))

def generate_io_trace(num_files, num_operations, block_size, seq_percent, output_file_io, output_file_meta):
    file_metadata = {}
    operations_count = {'read': 0, 'write': 0, 'create': 0}
    total_addressable_space = 0
    millis_counter = 0  # Compteur simulant les millisecondes

    with open(output_file_io, 'w') as io_f, open(output_file_meta, 'w') as meta_f:
        io_f.write("timestamp,requestType,filename,offsetStart,offsetEnd\n")  # En-tête du fichier d'I/O
        for _ in range(num_operations):
            millis_counter += 1
            file_id = f"File_{random.randint(1, num_files)}"
            is_sequential = random.random() < seq_percent / 100.0
            operation = 'read' if random.random() < 0.7 else 'write'  # 70% de probabilité de lire
            operations_count[operation] += 1

            if file_id not in file_metadata:
                # Tailles des fichiers suivant une distribution log-normale pour simuler de grands fichiers HPC
                size = int(np.random.lognormal(mean=13, sigma=0.5))
                file_metadata[file_id] = {'size': size, 'firstAccessTime': millis_counter, 'lastAccessTime': millis_counter}
                operation = 'create'  # Force l'opération à 'create' pour les nouveaux fichiers
                operations_count[operation] += 1
            else:
                file_metadata[file_id]['lastAccessTime'] = millis_counter
                size = file_metadata[file_id]['size']

            start_offset = 0 if operation == 'create' else random.randint(0, size - 1)
            end_offset = generate_sequential_ops(start_offset, size, block_size, is_sequential)
            total_addressable_space += end_offset - start_offset

            io_f.write(f"{millis_counter},{operation},{file_id},{start_offset},{end_offset}\n")

        for file_id, metadata in file_metadata.items():
            lifetime = metadata['lastAccessTime'] - metadata['firstAccessTime']
            meta_f.write(f"{file_id},{metadata['size']},{metadata['firstAccessTime']},{metadata['lastAccessTime']},{lifetime}\n")

    print(f"Espace adressable du workload: {total_addressable_space} octets")
    print("Répartition des opérations d'I/O :")
    for operation, count in operations_count.items():
        print(f"  {operation}: {count}")
    print("Génération des traces d'I/O pour un environnement HPC simulé terminée.")

num_files = 100
num_operations = 1000
block_size = 1024   # Blocs de 1 ko pour simuler les accès en HPC
seq_percent = 90  # 90% de probabilité pour les accès séquentiels
output_file_io = "io_trace_hpc.csv"
output_file_meta = "file_metadata_hpc.csv"

generate_io_trace(num_files, num_operations, block_size, seq_percent, output_file_io, output_file_meta)
