'''import random
import numpy as np

def generate_ops(start_offset, max_size, block_size, is_max_io_size):
    if is_max_io_size:
        end_offset = max_size
    else:
        end_offset = random.randint(start_offset, min(start_offset + block_size, max_size))
    blocks_accessed = (end_offset - start_offset) // block_size if start_offset != end_offset else 1
    return end_offset, blocks_accessed

def poisson_timestamp(lam):
    return np.random.poisson(lam)

def generate_file_size():
    if random.random() < 0.5:
        # Taille entre 20 et 50 MB
        size = np.random.lognormal(mean=3.5, sigma=0.3)  # Ajuster les paramètres pour obtenir la plage souhaitée
        return int(min(max(size, 20 * 1024 * 1024), 50 * 1024 * 1024))
    else:
        # Taille autour de 4.5 GB
        size = np.random.lognormal(mean=22, sigma=0.3)  # Ajuster les paramètres pour obtenir environ 4.5 GB
        return int(min(max(size, 4.2 * 1024 * 1024 * 1024), 4.8 * 1024 * 1024 * 1024))

def generate_io_trace(num_files, num_operations, block_size, output_file_io, output_file_meta,
                      num_files_with_zero_lifetime, num_users, max_io_size_percent):
    file_metadata = {}
    operations_count = {'read': 0, 'write': 0, 'create': 0}
    file_access_counts = {}
    file_blocks_accessed = {}

    total_addressable_space = 0
    total_file_size = 0
    millis_counter = 0

    user_files = distribute_files_among_users(num_files, num_users)

    with open(output_file_io, 'w') as io_f, open(output_file_meta, 'w') as meta_f:
        io_f.write("timestamp,requestType,filename,offsetStart,offsetEnd,user_id\n")
        meta_f.write("filename,size,firstAccessTime,lastAccessTime,lifetime,user_id\n")

        for _ in range(num_operations):
            millis_counter += poisson_timestamp(300)  # Utilisation de la distribution de Poisson pour les horodatages
            file_id_number = int(np.random.normal(loc=num_files/2, scale=num_files/4))
            file_id_number = min(max(1, file_id_number), num_files)
            file_id = f"File_{file_id_number}"
            user_id = (file_id_number - 1) % num_users + 1  # Associe chaque fichier à un utilisateur
            operation = 'read' if random.random() < 0.7 else 'write'

            if file_id not in file_metadata:
                size = generate_file_size()
                file_metadata[file_id] = {'size': size, 'firstAccessTime': millis_counter, 'lastAccessTime': millis_counter, 'user_id': user_id}
                total_file_size += size
                file_access_counts[file_id] = 1
                file_blocks_accessed[file_id] = 0
                io_f.write(f"{millis_counter},create,{file_id},0,{size},{user_id}\n")
                operations_count['create'] += 1
            else:
                file_metadata[file_id]['lastAccessTime'] = millis_counter
                file_access_counts[file_id] += 1
                operations_count[operation] += 1

            if operation != 'create':
                size = file_metadata[file_id]['size']
                is_max_io_size = random.random() < max_io_size_percent / 100.0
                start_offset = random.randint(0, size - 1)
                end_offset, blocks_accessed = generate_ops(start_offset, size, block_size, is_max_io_size)
                file_blocks_accessed[file_id] += blocks_accessed
                total_addressable_space += end_offset - start_offset
                io_f.write(f"{millis_counter},{operation},{file_id},{start_offset},{end_offset},{user_id}\n")

        for file_id, metadata in file_metadata.items():
            lifetime = metadata['lastAccessTime'] - metadata['firstAccessTime']
            if random.random() < 0.45:  # 45% des fichiers avec une durée de vie nulle
                lifetime = 0
            meta_f.write(f"{file_id},{metadata['size']},{metadata['firstAccessTime']},{metadata['lastAccessTime']},{lifetime},{metadata['user_id']}\n")

    print("Détails des accès par fichier :")
    for file_id, repetitions in file_access_counts.items():
        print(f"{file_id} a été accédé {repetitions} fois, avec {file_blocks_accessed[file_id]} blocs accédés.")

    print(f"Espace adressable du workload: {total_addressable_space} octets")
    print(f"Taille totale de tous les fichiers: {total_file_size} octets")
    print("Répartition des opérations d'I/O :")
    for operation, count in operations_count.items():
        print(f"  {operation}: {count}")

    print(f"Nombre total de fichiers: {num_files}")
    print(f"Nombre total d'utilisateurs: {num_users}")
    print("Génération des traces d'I/O pour un environnement HPC simulé terminée.")

def distribute_files_among_users(num_files, num_users):
    files_per_user = {f"User_{i+1}": [] for i in range(num_users)}
    file_sizes = np.array([generate_file_size() for _ in range(num_files)])
    total_file_size = sum(file_sizes)
    size_per_user = total_file_size // num_users

    file_indices = np.arange(num_files)
    np.random.shuffle(file_indices)

    current_user = 0
    current_size = 0

    for idx in file_indices:
        files_per_user[f"User_{current_user+1}"].append(f"File_{idx+1}")
        current_size += file_sizes[idx]
        if current_size >= size_per_user:
            current_user += 1
            current_size = 0
            if current_user >= num_users:
                current_user = 0

    return files_per_user

# Paramètres de simulation
num_files = 100  # Nombre de fichiers à créer
num_operations = 10000  # Nombre d'opérations d'I/O à générer
block_size = 1024  # Blocs de 1 ko pour simuler les accès en HPC
output_file_io = "data3/trace/io_trace_hpc.csv"  # Fichier de sortie pour les opérations d'I/O
output_file_meta = "data3/trace/file_metadata_hpc.csv"  # Fichier de sortie pour les métadonnées des fichiers
num_files_with_zero_lifetime = 45  # 45% des fichiers auront une durée de vie nulle
num_users = 10  # Nombre d'utilisateurs
max_io_size_percent = 50  # 50% des I/O auront la taille maximale du fichier

generate_io_trace(num_files, num_operations, block_size, output_file_io, output_file_meta, 
                  num_files_with_zero_lifetime, num_users, max_io_size_percent)'''



#################################################################################################
import random
import numpy as np

def generate_ops_gaussian(size, block_size, gauss_mean, gauss_std, is_max_io_size):
    if is_max_io_size:
        start_offset = 0
        end_offset = size
    else:
        start_offset = int(np.random.normal(loc=gauss_mean, scale=gauss_std))
        start_offset = min(max(0, start_offset), size - 1)
        end_offset = random.randint(start_offset, min(start_offset + block_size, size))
    blocks_accessed = (end_offset - start_offset) // block_size if start_offset != end_offset else 1
    return start_offset, end_offset, blocks_accessed

def exponential_timestamp(lam):
    return random.expovariate(1 / lam)

def generate_file_size():
    if random.random() < 0.9:
        size = np.random.lognormal(mean=3.5, sigma=0.3)
        return int(min(max(size, 20 * 1024 * 1024), 50 * 1024 * 1024))
    else:
        size = np.random.lognormal(mean=22, sigma=0.3)
        return int(min(max(size, 20 * 1024 * 1024), 50 * 1024 * 1024))

def generate_io_trace(num_files, num_operations, block_size, output_file_io, output_file_meta, max_io_size_percent, gauss_file_mean=None, gauss_file_std=None, gauss_offset_mean=None, gauss_offset_std=None):
    file_metadata = {}
    operations_count = {'read': 0, 'write': 0, 'create': 0}
    file_access_counts = {}
    file_blocks_accessed = {}

    total_addressable_space = 0
    total_file_size = 0
    millis_counter = 0

    if gauss_file_mean is None:
        gauss_file_mean = num_files / 2  # Par défaut, la moyenne est au centre des fichiers
    if gauss_file_std is None:
        gauss_file_std = num_files / 10  # Par défaut, l'écart-type est 1/10 du nombre de fichiers
    if gauss_offset_mean is None:
        gauss_offset_mean = 0.5  # Par défaut, la moyenne est au centre du fichier
    if gauss_offset_std is None:
        gauss_offset_std = 0.1  # Par défaut, l'écart-type est 1/10 de la taille du fichier

    with open(output_file_io, 'w') as io_f, open(output_file_meta, 'w') as meta_f:
        io_f.write("timestamp,requestType,filename,offsetStart,offsetEnd\n")
        meta_f.write("filename,size,firstAccessTime,lastAccessTime,lifetime\n")

        operations = []

        while len(operations) < num_operations:
            millis_counter += exponential_timestamp(300)

            if len(file_metadata) < num_files and random.random() < (num_files / num_operations):
                # Créer un nouveau fichier de manière plus uniforme
                file_id_number = len(file_metadata) + 1
                file_id = f"File_{file_id_number}"
                size = generate_file_size()
                file_metadata[file_id] = {'size': size, 'firstAccessTime': millis_counter, 'lastAccessTime': millis_counter}
                total_file_size += size
                file_access_counts[file_id] = 1
                file_blocks_accessed[file_id] = 0
                operations.append((millis_counter, 'create', file_id, 0, size))
                operations_count['create'] += 1
            else:
                file_id_number = int(np.random.normal(loc=gauss_file_mean, scale=gauss_file_std))
                file_id_number = min(max(1, file_id_number), num_files)
                file_id = f"File_{file_id_number}"
                if file_id not in file_metadata:
                    continue

                operation = 'read' if random.random() < 0.7 else 'write'
                file_metadata[file_id]['lastAccessTime'] = millis_counter
                file_access_counts[file_id] += 1
                operations_count[operation] += 1

                size = file_metadata[file_id]['size']
                is_max_io_size = random.random() < max_io_size_percent / 100.0
                gauss_mean_offset = gauss_offset_mean * size
                gauss_std_offset = gauss_offset_std * size
                start_offset, end_offset, blocks_accessed = generate_ops_gaussian(size, block_size, gauss_mean_offset, gauss_std_offset, is_max_io_size)
                file_blocks_accessed[file_id] += blocks_accessed
                total_addressable_space += end_offset - start_offset
                operations.append((millis_counter, operation, file_id, start_offset, end_offset))

        # Tri des opérations par timestamp
        operations.sort(key=lambda x: x[0])
        for op in operations:
            io_f.write(f"{op[0]},{op[1]},{op[2]},{op[3]},{op[4]}\n")

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

# Paramètres de simulation
num_files = 50  # Nombre de fichiers à créer
num_operations = 1000  # Nombre d'opérations d'I/O à générer
block_size = 1024  # Blocs de 1 ko pour simuler les accès en HPC
output_file_io = "data3/trace_arc/io_trace_hpc.csv"  # Fichier de sortie pour les opérations d'I/O
output_file_meta = "data3/trace_arc/file_metadata_hpc.csv"  # Fichier de sortie pour les métadonnées des fichiers
max_io_size_percent = 0  # 0% des I/O auront la taille maximale du fichier

# Utilisation des paramètres de la gaussienne pour la sélection des fichiers
gauss_file_mean = num_files / 2  # La moyenne est centrée sur le milieu des fichiers
gauss_file_std = num_files / 10  # L'écart-type est plus petit pour rendre la courbe plus pointue

# Utilisation des paramètres de la gaussienne pour les offsets
gauss_offset_mean = 0.001  # 
gauss_offset_std = 0.001  # 

generate_io_trace(num_files, num_operations, block_size, output_file_io, output_file_meta, max_io_size_percent, gauss_file_mean, gauss_file_std, gauss_offset_mean, gauss_offset_std)
########################################################################################for no arc########################################################################################
'''import random
import numpy as np

def generate_ops_full(size, block_size):
    start_offset = 0
    end_offset = size
    blocks_accessed = size // block_size if start_offset != end_offset else 1
    return start_offset, end_offset, blocks_accessed

def exponential_timestamp(lam):
    return random.expovariate(1 / lam)

def generate_large_file_size():
    size = random.choice([1 * 1024 * 1024 * 1024, 2 * 1024 * 1024 * 1024])  # 1 Go ou 2 Go
    return size

def generate_io_trace_full_io(num_files, num_operations, block_size, output_file_io, output_file_meta):
    file_metadata = {}
    operations_count = {'read': 0, 'write': 0, 'create': 0}
    file_access_counts = {}
    file_blocks_accessed = {}

    total_addressable_space = 0
    total_file_size = 0
    millis_counter = 0

    with open(output_file_io, 'w') as io_f, open(output_file_meta, 'w') as meta_f:
        io_f.write("timestamp,requestType,filename,offsetStart,offsetEnd\n")
        meta_f.write("filename,size,firstAccessTime,lastAccessTime,lifetime\n")

        operations = []
        file_creation_intervals = num_operations // num_files

        while len(operations) < num_operations:
            millis_counter += exponential_timestamp(300)

            if len(file_metadata) < num_files and len(operations) % file_creation_intervals == 0:
                # Créer un nouveau fichier de manière plus uniforme
                file_id_number = len(file_metadata) + 1
                file_id = f"File_{file_id_number}"
                size = generate_large_file_size()
                file_metadata[file_id] = {'size': size, 'firstAccessTime': millis_counter, 'lastAccessTime': millis_counter}
                total_file_size += size
                file_access_counts[file_id] = 1
                file_blocks_accessed[file_id] = size // block_size
                operations.append((millis_counter, 'create', file_id, 0, size))
                operations_count['create'] += 1
            else:
                file_id_number = random.randint(1, num_files)
                file_id = f"File_{file_id_number}"
                if file_id not in file_metadata:
                    continue

                operation = 'read' if random.random() < 0.7 else 'write'
                file_metadata[file_id]['lastAccessTime'] = millis_counter
                file_access_counts[file_id] += 1
                operations_count[operation] += 1

                size = file_metadata[file_id]['size']
                start_offset, end_offset, blocks_accessed = generate_ops_full(size, block_size)
                file_blocks_accessed[file_id] += blocks_accessed
                total_addressable_space += end_offset - start_offset
                operations.append((millis_counter, operation, file_id, start_offset, end_offset))

        # Tri des opérations par timestamp
        operations.sort(key=lambda x: x[0])
        for op in operations:
            io_f.write(f"{op[0]},{op[1]},{op[2]},{op[3]},{op[4]}\n")

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

# Paramètres de simulation pour des grandes requêtes sur l'ensemble du fichier
num_files = 50
num_operations = 1000
block_size = 1024
output_file_io = "data3/trace_full/io_trace_hpc.csv"
output_file_meta = "data3/trace_full/file_metadata_hpc.csv"

generate_io_trace_full_io(num_files, num_operations, block_size, output_file_io, output_file_meta)'''

###################################################################### for arc########################################################################################  
'''import random
import numpy as np

def generate_ops_small_block(size, block_size):
    start_offset = random.randint(0, size - block_size)
    end_offset = min(start_offset + block_size, size)
    blocks_accessed = 1 if start_offset != end_offset else 1
    return start_offset, end_offset, blocks_accessed

def exponential_timestamp(lam):
    return random.expovariate(1 / lam)

def generate_large_file_size():
    size = random.choice([20 * 1024 * 1024 , 50 * 1024 * 1024])  # 1 Go ou 2 Go
    return size

def generate_io_trace_small_blocks(num_files, num_operations, block_size, output_file_io, output_file_meta):
    file_metadata = {}
    operations_count = {'read': 0, 'write': 0, 'create': 0}
    file_access_counts = {}
    file_blocks_accessed = {}

    total_addressable_space = 0
    total_file_size = 0
    millis_counter = 0

    with open(output_file_io, 'w') as io_f, open(output_file_meta, 'w') as meta_f:
        io_f.write("timestamp,requestType,filename,offsetStart,offsetEnd\n")
        meta_f.write("filename,size,firstAccessTime,lastAccessTime,lifetime\n")

        operations = []
        file_creation_intervals = num_operations // num_files

        while len(operations) < num_operations:
            millis_counter += exponential_timestamp(300)

            if len(file_metadata) < num_files and len(operations) % file_creation_intervals == 0:
                # Créer un nouveau fichier de manière plus uniforme
                file_id_number = len(file_metadata) + 1
                file_id = f"File_{file_id_number}"
                size = generate_large_file_size()
                file_metadata[file_id] = {'size': size, 'firstAccessTime': millis_counter, 'lastAccessTime': millis_counter}
                total_file_size += size
                file_access_counts[file_id] = 1
                file_blocks_accessed[file_id] = size // block_size
                operations.append((millis_counter, 'create', file_id, 0, size))
                operations_count['create'] += 1
            else:
                file_id_number = random.randint(1, num_files)
                file_id = f"File_{file_id_number}"
                if file_id not in file_metadata:
                    continue

                operation = 'read' if random.random() < 0.7 else 'write'
                file_metadata[file_id]['lastAccessTime'] = millis_counter
                file_access_counts[file_id] += 1
                operations_count[operation] += 1

                size = file_metadata[file_id]['size']
                start_offset, end_offset, blocks_accessed = generate_ops_small_block(size, block_size)
                file_blocks_accessed[file_id] += blocks_accessed
                total_addressable_space += end_offset - start_offset
                operations.append((millis_counter, operation, file_id, start_offset, end_offset))

        # Tri des opérations par timestamp
        operations.sort(key=lambda x: x[0])
        for op in operations:
            io_f.write(f"{op[0]},{op[1]},{op[2]},{op[3]},{op[4]}\n")

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

# Paramètres de simulation pour des petites requêtes sur un sous-ensemble de blocs
num_files = 60
num_operations = 1000
block_size = 1024
output_file_io = "data3/trace_small/io_trace_hpc.csv"
output_file_meta = "data3/trace_small/file_metadata_hpc.csv"

generate_io_trace_small_blocks(num_files, num_operations, block_size, output_file_io, output_file_meta)'''
