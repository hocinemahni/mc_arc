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
                    continue  # Ignore malformed lines
                timestamp, op_code_full, uid = int(split_line[0]) / 1000, split_line[1], split_line[2]
                size = int(split_line[3]) if len(split_line) > 3 else 0
                offset_start = int(split_line[4]) if len(split_line) > 4 else 0
                offset_end = int(split_line[5]) if len(split_line) > 5 else size

                op_code = op_code_full.split('.')[1]

                if ignore_head and op_code == "HEAD":
                    continue

                if op_code in ["COPY", "DELETE"]:
                    continue

                if uid not in file_ids_metadata:
                    file_ids_metadata[uid] = [timestamp, timestamp, size]  # [first_access_time, last_access_time, size]
                else:
                    file_ids_metadata[uid][1] = timestamp  # Update last access time

                all_operations.append((timestamp, op_code, uid, offset_start, offset_end))

    # Add lifetime calculation for each file
    for uid, data in file_ids_metadata.items():
        first_access, last_access, size = data
        lifetime = last_access - first_access
        file_ids_metadata[uid].append(lifetime)

    return all_operations, file_ids_metadata

def save_data_to_csv(data, file_name, column_names):
    df = pd.DataFrame(data, columns=column_names)
    df.to_csv(file_name, index=False)

def save_lifetimes_figure(file_ids_metadata, output_directory):
    lifetimes = [data[3] for data in file_ids_metadata.values()]  # Extract lifetime

    sorted_lifetimes = sorted(lifetimes)

    log_lifetimes = [np.log10(lifetime + 1) for lifetime in sorted_lifetimes]

    y_values = np.arange(1, len(log_lifetimes) + 1) / len(log_lifetimes)

    plt.figure(figsize=(10, 6))
    plt.plot(log_lifetimes, y_values, marker='.', linestyle='-')

    plt.title('Distribution Cumulative des Durées de Vie des Fichiers')
    plt.xlabel('Durée de Vie (log10 secondes)')
    plt.ylabel('Proportion Cumulative des Fichiers')

    plt.grid(True)

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    plt.savefig(os.path.join(output_directory, 'lifetime_distribution.png'))
    plt.close()

# Example usage
file_paths = ['IBMObjectStoreTrace007Part0.csv']
output_directory = 'data3/'
output_directory_images = 'output/'
all_operations, file_ids_metadata = parse_trace_files(file_paths)
save_data_to_csv(all_operations, 'data3/IBMObjectStoreTrace007Part0_request.csv', ['timestamp', 'requestType', 'filename', 'offsetStart', 'offsetEnd'])
save_data_to_csv([(uid, data[2], data[0], data[1], data[3]) for uid, data in file_ids_metadata.items()], 'data3/IBMObjectStoreTrace007Part0_metadata.csv', ['filename', 'size', 'firstAccessTime', 'lastAccessTime', 'lifetime'])
save_lifetimes_figure(file_ids_metadata, output_directory_images)
