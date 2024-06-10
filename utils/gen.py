# -*- coding: utf-8 -*-
import random
import numpy as np


def generate_timestamp(lam):
     return random.expovariate(1/lam)
     #return np.random.poisson(1/lam)

def select_file(num_files, gauss_mean, gauss_std):
    file_index = int(np.random.normal(gauss_mean, gauss_std))
    return max(1, min(num_files, file_index))

"""def select_block(size, block_size):
    '''gauss_mean = size * 0.2
    gauss_std = size * 0.1'''
    gauss_mean = 0.1
    gauss_std = 0.1
    start_offset = int(np.random.normal(gauss_mean, gauss_std))
    start_offset = max(0, min(size - block_size, start_offset))
    #end_offset = min(start_offset + block_size, size)
    end_offset = size
    return start_offset, end_offset"""

def select_block(size):
    # Configuration pour start_offset
    start_mean = 0.1
    start_std = 0.05
    start_offset = int(np.random.normal(start_mean, start_std))
    start_offset = max(0, min(start_offset, size))

    # Configuration pour end_offset
    end_mean = size * 0.9  
    end_std = size * 0.35  
    end_offset = int(np.random.normal(end_mean, end_std))
    end_offset = max(start_offset, min(end_offset, size)) 
    return start_offset, end_offset

def generate_operations(num_files, num_operations, block_size, op_ratio):
    file_sizes = [random.randint(1, 500) * 1024 for _ in range(num_files)]  # Tailles des fichiers en KB
    timestamps = sorted(generate_timestamp(200) for _ in range(num_operations))
    operations = []
    file_metadata = {}

    for timestamp in timestamps:
        file_id = select_file(num_files, 50, 15)  # Sélection gaussienne des fichiers
        file_size = file_sizes[file_id - 1]
        start, end = select_block(file_size)  # Sélection gaussienne des blocs
        op_type = "read" if random.random() < op_ratio else "write"

        if file_id not in file_metadata:
            file_metadata[file_id] = {'size': file_size, 'firstAccessTime': timestamp, 'lastAccessTime': timestamp}
        else:
            file_metadata[file_id]['lastAccessTime'] = timestamp

        operations.append((timestamp, op_type, f"file_{file_id}", start, end))

    for file_id in file_metadata:
        file_metadata[file_id]['lifetime'] = file_metadata[file_id]['lastAccessTime'] - file_metadata[file_id]['firstAccessTime']

    return operations, file_metadata


def write_to_csv(file_path, data, header):
    with open(file_path, 'w') as file:
        file.write(header + '\n')
        for row in data:
            file.write(','.join(map(str, row)) + '\n')


# Simulation parameters
num_files = 100  # Number of files in the system
num_operations = 5000  # Number of I/O operations to simulate
block_size = 1024  # Block size in bytes
op_ratio = 0.7  # Ratio of read operations

# Generate I/O operations and file metadata
operations, file_metadata = generate_operations(num_files, num_operations, block_size, op_ratio)

# Write operations to CSV file
io_csv_path = "data3/trace_no_arc/io_operations.csv"
io_csv_header = "timestamp,requestType,filename,offsetStart,offsetEnd"
write_to_csv(io_csv_path, operations, io_csv_header)

# Write metadata to CSV file
metadata_csv_path = "data3/trace_no_arc/file_metadata.csv"
metadata_csv_header = "filename,size,firstAccessTime,lastAccessTime,lifetime"
metadata_data = [(f"file_{file_id}", meta['size'], meta['firstAccessTime'], meta['lastAccessTime'], meta['lifetime'])
                 for file_id, meta in file_metadata.items()]
write_to_csv(metadata_csv_path, metadata_data, metadata_csv_header)

print(f"Generated I/O operations are written to {io_csv_path}")
print(f"File metadata is written to {metadata_csv_path}")

