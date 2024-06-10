'''import random
import numpy as np

def generate_ops(start_offset, max_size, block_size, is_max_io_size):
    if is_max_io_size:
        start_offset = 0
        end_offset = max_size
    else:
        end_offset = random.randint(start_offset, min(start_offset + block_size, max_size))
    blocks_accessed = (end_offset - start_offset) // block_size if start_offset != end_offset else 1
    return start_offset, end_offset, blocks_accessed

def poisson_timestamp(lam):
    return np.random.poisson(lam)

def generate_file_size():
    if random.random() < 0.7:
        size = np.random.lognormal(mean=3.5, sigma=0.3)
        return int(min(max(size, 20 * 1024 * 1024), 50 * 1024 * 1024))
    else:
        size = np.random.lognormal(mean=22, sigma=0.3)
        return int(min(max(size, 4.2 * 1024 * 1024 * 1024), 4.8 * 1024 * 1024 * 1024))

def generate_io_trace(num_files, num_operations, block_size, output_file_io, output_file_meta, max_io_size_percent):
    file_metadata = {}
    operations_count = {'read': 0, 'write': 0, 'create': 0}
    file_access_counts = {}
    file_blocks_accessed = {}

    total_addressable_space = 0
    total_file_size = 0
    millis_counter = 0
    files_created = 0

    with open(output_file_io, 'w') as io_f, open(output_file_meta, 'w') as meta_f:
        io_f.write("timestamp,requestType,filename,offsetStart,offsetEnd\n")
        meta_f.write("filename,size,firstAccessTime,lastAccessTime,lifetime\n")

        for _ in range(num_operations):
            millis_counter += poisson_timestamp(300)
            if files_created < num_files and random.random() < 0.5:
                file_id_number = files_created + 1
                file_id = f"File_{file_id_number}"
                size = generate_file_size()
                file_metadata[file_id] = {'size': size, 'firstAccessTime': millis_counter, 'lastAccessTime': millis_counter}
                total_file_size += size
                file_access_counts[file_id] = 1
                file_blocks_accessed[file_id] = 0
                io_f.write(f"{millis_counter},create,{file_id},0,{size}\n")
                operations_count['create'] += 1
                files_created += 1
            else:
                file_id_number = int(np.random.normal(loc=num_files / 2, scale=num_files / 4))
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
                start_offset, end_offset, blocks_accessed = generate_ops(0 if is_max_io_size else random.randint(0, size - 1), size, block_size, is_max_io_size)
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

# Paramètres de simulation
num_files = 20  # Nombre de fichiers à créer
num_operations = 200  # Nombre d'opérations d'I/O à générer
block_size = 1024  # Blocs de 1 ko pour simuler les accès en HPC
output_file_io = "data3/trace/100/io_trace_hpc.csv"  # Fichier de sortie pour les opérations d'I/O
output_file_meta = "data3/trace/100/file_metadata_hpc.csv"  # Fichier de sortie pour les métadonnées des fichiers
max_io_size_percent = 100  # 100% des I/O auront la taille maximale du fichier

generate_io_trace(num_files, num_operations, block_size, output_file_io, output_file_meta, max_io_size_percent)'''
"""import random
import numpy as np

def generate_ops(start_offset, max_size, block_size, is_max_io_size):
    if is_max_io_size:
        start_offset = 0
        end_offset = max_size
    else:
        end_offset = random.randint(start_offset, min(start_offset + block_size, max_size))
    blocks_accessed = (end_offset - start_offset) // block_size if start_offset != end_offset else 1
    return start_offset, end_offset, blocks_accessed

def poisson_timestamp(lam):
    return np.random.poisson(lam)

def generate_file_size():
    if random.random() < 0.95:
        size = np.random.lognormal(mean=3.5, sigma=0.3)
        return int(min(max(size, 20 * 1024 * 1024), 50 * 1024 * 1024))
    else:
        size = np.random.lognormal(mean=22, sigma=0.3)
        return int(min(max(size, 1 * 1024 * 1024 * 1024), 2 * 1024 * 1024 * 1024))

def generate_io_trace(num_files, num_operations, block_size, output_file_io, output_file_meta, max_io_size_percent):
    file_metadata = {}
    operations_count = {'read': 0, 'write': 0, 'create': 0}
    file_access_counts = {}
    file_blocks_accessed = {}

    total_addressable_space = 0
    total_file_size = 0
    millis_counter = 0
    files_created = 0

    # Sélection des fichiers qui auront uniquement des opérations de création
    create_only_files = set(random.sample(range(1, num_files + 1), int(0.45 * num_files)))

    with open(output_file_io, 'w') as io_f, open(output_file_meta, 'w') as meta_f:
        io_f.write("timestamp,requestType,filename,offsetStart,offsetEnd\n")
        meta_f.write("filename,size,firstAccessTime,lastAccessTime,lifetime\n")

        for _ in range(num_operations):
            millis_counter += poisson_timestamp(300)
            if files_created < num_files and random.random() < 0.5:
                file_id_number = files_created + 1
                file_id = f"File_{file_id_number}"
                size = generate_file_size()
                file_metadata[file_id] = {'size': size, 'firstAccessTime': millis_counter, 'lastAccessTime': millis_counter}
                total_file_size += size
                file_access_counts[file_id] = 1
                file_blocks_accessed[file_id] = 0
                io_f.write(f"{millis_counter},create,{file_id},0,{size}\n")
                operations_count['create'] += 1
                files_created += 1
            else:
                file_id_number = int(np.random.normal(loc=num_files / 2, scale=num_files / 4))
                file_id_number = min(max(1, file_id_number), num_files)
                file_id = f"File_{file_id_number}"
                if file_id not in file_metadata or file_id_number in create_only_files:
                    continue

                operation = 'read' if random.random() < 0.7 else 'write'
                file_metadata[file_id]['lastAccessTime'] = millis_counter
                file_access_counts[file_id] += 1
                operations_count[operation] += 1

                size = file_metadata[file_id]['size']
                is_max_io_size = random.random() < max_io_size_percent / 100.0
                start_offset, end_offset, blocks_accessed = generate_ops(0 if is_max_io_size else random.randint(0, size - 1), size, block_size, is_max_io_size)
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

# Paramètres de simulation
num_files = 20  # Nombre de fichiers à créer
num_operations = 500  # Nombre d'opérations d'I/O à générer
block_size = 1024  # Blocs de 1 ko pour simuler les accès en HPC
output_file_io = "data3/trace/100/io_trace_hpc.csv"  # Fichier de sortie pour les opérations d'I/O
output_file_meta = "data3/trace/100/file_metadata_hpc.csv"  # Fichier de sortie pour les métadonnées des fichiers
max_io_size_percent = 100  # 100% des I/O auront la taille maximale du fichier

generate_io_trace(num_files, num_operations, block_size, output_file_io, output_file_meta, max_io_size_percent)"""
'''import random
import numpy as np

def generate_ops(start_offset, max_size, block_size, is_max_io_size):
    if is_max_io_size:
        start_offset = 0
        end_offset = max_size
    else:
        end_offset = random.randint(start_offset, min(start_offset + block_size, max_size))
    blocks_accessed = (end_offset - start_offset) // block_size if start_offset != end_offset else 1
    return start_offset, end_offset, blocks_accessed

def poisson_timestamp(lam):
    return np.random.poisson(lam)

def generate_file_size():
    if random.random() < 0.7:
        size = np.random.lognormal(mean=3.5, sigma=0.3)
        return int(min(max(size, 20 * 1024 * 1024), 50 * 1024 * 1024))
    else:
        size = np.random.lognormal(mean=22, sigma=0.3)
        return int(min(max(size, 4.2 * 1024 * 1024 * 1024), 4.8 * 1024 * 1024 * 1024))

def generate_io_trace(num_files, num_operations, block_size, output_file_io, output_file_meta, max_io_size_percent):
    file_metadata = {}
    operations_count = {'read': 0, 'write': 0, 'create': 0}
    file_access_counts = {}
    file_blocks_accessed = {}

    total_addressable_space = 0
    total_file_size = 0
    millis_counter = 0
    files_created = 0

    # Sélection des fichiers qui auront uniquement des opérations de création
    create_only_files = set(random.sample(range(1, num_files + 1), int(0.45 * num_files)))

    with open(output_file_io, 'w') as io_f, open(output_file_meta, 'w') as meta_f:
        io_f.write("timestamp,requestType,filename,offsetStart,offsetEnd\n")
        meta_f.write("filename,size,firstAccessTime,lastAccessTime,lifetime\n")

        operations = []

        while len(operations) < num_operations:
            millis_counter += poisson_timestamp(300)

            # Utilisation de la distribution gaussienne pour sélectionner un fichier
            file_id_number = int(np.random.normal(loc=num_files / 2, scale=num_files / 4))
            file_id_number = min(max(1, file_id_number), num_files)
            file_id = f"File_{file_id_number}"

            if files_created < num_files and file_id_number not in file_metadata:
                # Créer un nouveau fichier si le nombre de fichiers créés est encore inférieur au total requis
                size = generate_file_size()
                file_metadata[file_id] = {'size': size, 'firstAccessTime': millis_counter, 'lastAccessTime': millis_counter}
                total_file_size += size
                file_access_counts[file_id] = 1
                file_blocks_accessed[file_id] = 0
                operations.append((millis_counter, 'create', file_id, 0, size))
                operations_count['create'] += 1
                files_created += 1
            elif file_id in file_metadata and file_id_number not in create_only_files:
                # Effectuer une opération de lecture ou d'écriture sur un fichier existant
                operation = 'read' if random.random() < 0.7 else 'write'
                file_metadata[file_id]['lastAccessTime'] = millis_counter
                file_access_counts[file_id] += 1
                operations_count[operation] += 1

                size = file_metadata[file_id]['size']
                is_max_io_size = random.random() < max_io_size_percent / 100.0
                start_offset, end_offset, blocks_accessed = generate_ops(0 if is_max_io_size else random.randint(0, size - 1), size, block_size, is_max_io_size)
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
num_files = 20  # Nombre de fichiers à créer
num_operations = 200  # Nombre d'opérations d'I/O à générer
block_size = 1024  # Blocs de 1 ko pour simuler les accès en HPC
output_file_io = "data3/trace/100/io_trace_hpc.csv"  # Fichier de sortie pour les opérations d'I/O
output_file_meta = "data3/trace/100/file_metadata_hpc.csv"  # Fichier de sortie pour les métadonnées des fichiers
max_io_size_percent = 100  # 100% des I/O auront la taille maximale du fichier

generate_io_trace(num_files, num_operations, block_size, output_file_io, output_file_meta, max_io_size_percent)'''

"""import random
import numpy as np

def generate_ops(start_offset, max_size, block_size, is_max_io_size):
    if is_max_io_size:
        start_offset = 0
        end_offset = max_size
    else:
        end_offset = random.randint(start_offset, min(start_offset + block_size, max_size))
    blocks_accessed = (end_offset - start_offset) // block_size if start_offset != end_offset else 1
    return start_offset, end_offset, blocks_accessed

def poisson_timestamp(lam):
    return np.random.poisson(lam)

def generate_file_size():
    if random.random() < 0.7:
        size = np.random.lognormal(mean=3.5, sigma=0.3)
        return int(min(max(size, 20 * 1024 * 1024), 50 * 1024 * 1024))
    else:
        size = np.random.lognormal(mean=22, sigma=0.3)
        return int(min(max(size, 4.2 * 1024 * 1024 * 1024), 4.8 * 1024 * 1024 * 1024))

def generate_io_trace(num_files, num_operations, block_size, output_file_io, output_file_meta, max_io_size_percent, gauss_mean=None, gauss_std=None):
    file_metadata = {}
    operations_count = {'read': 0, 'write': 0, 'create': 0}
    file_access_counts = {}
    file_blocks_accessed = {}

    total_addressable_space = 0
    total_file_size = 0
    millis_counter = 0

    # Sélection des fichiers qui auront uniquement des opérations de création
    create_only_files = set(random.sample(range(1, num_files + 1), int(0.45 * num_files)))

    if gauss_mean is None:
        gauss_mean = num_files / 2  # Par défaut, la moyenne est au centre
    if gauss_std is None:
        gauss_std = num_files / 10  # Par défaut, l'écart-type est 1/10 du nombre de fichiers

    with open(output_file_io, 'w') as io_f, open(output_file_meta, 'w') as meta_f:
        io_f.write("timestamp,requestType,filename,offsetStart,offsetEnd\n")
        meta_f.write("filename,size,firstAccessTime,lastAccessTime,lifetime\n")

        operations = []
        files_created = set()

        while len(operations) < num_operations:
            millis_counter += poisson_timestamp(300)
            file_id_number = int(np.random.normal(loc=gauss_mean, scale=gauss_std))
            file_id_number = min(max(1, file_id_number), num_files)
            file_id = f"File_{file_id_number}"

            if file_id not in file_metadata:
                size = generate_file_size()
                file_metadata[file_id] = {'size': size, 'firstAccessTime': millis_counter, 'lastAccessTime': millis_counter}
                total_file_size += size
                file_access_counts[file_id] = 1
                file_blocks_accessed[file_id] = 0
                operations.append((millis_counter, 'create', file_id, 0, size))
                operations_count['create'] += 1
                files_created.add(file_id_number)
            else:
                if file_id_number in create_only_files:
                    continue

                operation = 'read' if random.random() < 0.7 else 'write'
                file_metadata[file_id]['lastAccessTime'] = millis_counter
                file_access_counts[file_id] += 1
                operations_count[operation] += 1

                size = file_metadata[file_id]['size']
                is_max_io_size = random.random() < max_io_size_percent / 100.0
                start_offset, end_offset, blocks_accessed = generate_ops(0 if is_max_io_size else random.randint(0, size - 1), size, block_size, is_max_io_size)
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
num_operations = 500  # Nombre d'opérations d'I/O à générer
block_size = 1024  # Blocs de 1 ko pour simuler les accès en HPC
output_file_io = "data3/trace/100/io_trace_hpc.csv"  # Fichier de sortie pour les opérations d'I/O
output_file_meta = "data3/trace/100/file_metadata_hpc.csv"  # Fichier de sortie pour les métadonnées des fichiers
max_io_size_percent = 100  # 100% des I/O auront la taille maximale du fichier

# Utilisation des paramètres de la gaussienne
gauss_mean = num_files / 2  # La moyenne est centrée sur le milieu des fichiers
gauss_std = num_files / 10  # L'écart-type est plus petit pour rendre la courbe plus pointue

generate_io_trace(num_files, num_operations, block_size, output_file_io, output_file_meta, max_io_size_percent, gauss_mean, gauss_std)"""

"""import random
import numpy as np

def generate_ops(start_offset, max_size, block_size, is_max_io_size):
    if is_max_io_size:
        start_offset = 0
        end_offset = max_size
    else:
        end_offset = random.randint(start_offset, min(start_offset + block_size, max_size))
    blocks_accessed = (end_offset - start_offset) // block_size if start_offset != end_offset else 1
    return start_offset, end_offset, blocks_accessed

def poisson_timestamp(lam):
    return np.random.poisson(lam)

def generate_file_size():
    if random.random() < 0.9:
        size = np.random.lognormal(mean=3.5, sigma=0.3)
        return int(min(max(size, 20 * 1024 * 1024), 50 * 1024 * 1024))
    else:
        size = np.random.lognormal(mean=22, sigma=0.3)
        return int(min(max(size, 50 * 1024 * 1024 ), 3 * 1024 * 1024 * 1024))

def generate_io_trace(num_files, num_operations, block_size, output_file_io, output_file_meta, max_io_size_percent, gauss_mean=None, gauss_std=None):
    file_metadata = {}
    operations_count = {'read': 0, 'write': 0, 'create': 0}
    file_access_counts = {}
    file_blocks_accessed = {}

    total_addressable_space = 0
    total_file_size = 0
    millis_counter = 0

    if gauss_mean is None:
        gauss_mean = num_files / 2  # Par défaut, la moyenne est au centre
    if gauss_std is None:
        gauss_std = num_files / 10  # Par défaut, l'écart-type est 1/10 du nombre de fichiers

    with open(output_file_io, 'w') as io_f, open(output_file_meta, 'w') as meta_f:
        io_f.write("timestamp,requestType,filename,offsetStart,offsetEnd\n")
        meta_f.write("filename,size,firstAccessTime,lastAccessTime,lifetime\n")

        operations = []

        while len(operations) < num_operations:
            millis_counter += poisson_timestamp(300)

            if len(file_metadata) < num_files and random.random() < 0.5:
                # Créer un nouveau fichier
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
                # Sélectionner un fichier existant
                file_id_number = int(np.random.normal(loc=gauss_mean, scale=gauss_std))
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
                start_offset, end_offset, blocks_accessed = generate_ops(0 if is_max_io_size else random.randint(0, size - 1), size, block_size, is_max_io_size)
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
output_file_io = "data3/trace/0/io_trace_hpc.csv"  # Fichier de sortie pour les opérations d'I/O
output_file_meta = "data3/trace/0/file_metadata_hpc.csv"  # Fichier de sortie pour les métadonnées des fichiers
max_io_size_percent = 0  # 100% des I/O auront la taille maximale du fichier

# Utilisation des paramètres de la gaussienne
gauss_mean = num_files / 2  # La moyenne est centrée sur le milieu des fichiers
gauss_std = num_files / 10  # L'écart-type est plus petit pour rendre la courbe plus pointue

generate_io_trace(num_files, num_operations, block_size, output_file_io, output_file_meta, max_io_size_percent, gauss_mean, gauss_std)""" #fonctionne bien



'''import random
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

def poisson_timestamp(lam):
    return np.random.poisson(lam)

def generate_file_size():
    if random.random() < 0.9:
        size = np.random.lognormal(mean=3.5, sigma=0.3)
        return int(min(max(size, 20 * 1024 * 1024), 50 * 1024 * 1024))
    else:
        size = np.random.lognormal(mean=22, sigma=0.3)
        return int(min(max(size, 50 * 1024 * 1024 ), 3 * 1024 * 1024 * 1024))

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
            millis_counter += poisson_timestamp(300)

            if len(file_metadata) < num_files and random.random() < 0.5:
                # Créer un nouveau fichier
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
                # Sélectionner un fichier existant
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
output_file_io = "data3/trace/0/io_trace_hpc.csv"  # Fichier de sortie pour les opérations d'I/O
output_file_meta = "data3/trace/0/file_metadata_hpc.csv"  # Fichier de sortie pour les métadonnées des fichiers
max_io_size_percent = 0  # 0% des I/O auront la taille maximale du fichier

# Utilisation des paramètres de la gaussienne pour la sélection des fichiers
gauss_file_mean = num_files / 2  # La moyenne est centrée sur le milieu des fichiers
gauss_file_std = num_files / 10  # L'écart-type est plus petit pour rendre la courbe plus pointue

# Utilisation des paramètres de la gaussienne pour les offsets
gauss_offset_mean = 0.5  # La moyenne est centrée sur le milieu du fichier
gauss_offset_std = 0.1  # L'écart-type est une fraction de la taille du fichier

generate_io_trace(num_files, num_operations, block_size, output_file_io, output_file_meta, max_io_size_percent, gauss_file_mean, gauss_file_std, gauss_offset_mean, gauss_offset_std)'''
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
        return int(min(max(size, 50 * 1024 * 1024), 3 * 1024 * 1024 * 1024))

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

            file_id_number = int(np.random.normal(loc=gauss_file_mean, scale=gauss_file_std))
            file_id_number = min(max(1, file_id_number), num_files)
            file_id = f"File_{file_id_number}"

            if file_id not in file_metadata :
                # Créer un nouveau fichier
                size = generate_file_size()
                file_metadata[file_id] = {'size': size, 'firstAccessTime': millis_counter, 'lastAccessTime': millis_counter}
                total_file_size += size
                file_access_counts[file_id] = 1
                file_blocks_accessed[file_id] = 0
                operations.append((millis_counter, 'create', file_id, 0, size))
                operations_count['create'] += 1
            else:
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
gauss_offset_mean = 0.5  # La moyenne est centrée sur le milieu du fichier
gauss_offset_std = 0.1  # L'écart-type est une fraction de la taille du fichier

generate_io_trace(num_files, num_operations, block_size, output_file_io, output_file_meta, max_io_size_percent, gauss_file_mean, gauss_file_std, gauss_offset_mean, gauss_offset_std)
