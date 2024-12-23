# -*- coding: utf-8 -*-
import datetime
import math
import os
import csv
import random
import numpy as np
import matplotlib.pyplot as plt
from collections import deque, defaultdict

from structures.io import IORequest
from structures.tier import Tier
from structures.file import File
from structures.user import User

# Import caching policies (all imports are kept, but can be reorganized)
from policy.Idle_Time_BFH_ARC import Idle_Time_BFH_ARC
from policy.Idle_Time_BFH_ARC_whithiout_size import Idle_Time_BFH_ARC_whithiout_size
from policy.arcblock import Arc_block_Cache
from policy.mc_arc_topsis import BFH_topsis_bb
from policy.RLT_EQUITE import RLT_ARC_EQT
from policy.equité import equite
from policy.WSM_position_equite import position_equite
from policy.wsm import BFH_wsm
from policy.topsis import BFH_topsis
from policy.wsm1 import BFH_wsm1
from policy.mc_arc_aprescorrection import BFH_wsm1_mc_arc
from policy.wsm2 import BFH_wsm2
from policy.mc_arc_mod import mc_arc_mod
from policy.lru1 import LRU
from policy.lru2 import LRU2
from policy.fifo import FIFO
from policy.lfu import LFU
from policy.FG_ARC import FG_ARC
from policy.mc_arc_bb import mc_arc_bb
from policy.LRU_bb import LRU_block_bb
from policy.BFH_ARC import BFH_ARC
from policy.arc_bb import Arc_block_Cache
from policy.lfu_bb_l import LFU_bb_l
from policy.rlt_whithout_score import RLT
from policy.RLT_EQUITE_wsm import RLT_ARC_EQT_wms
from policy.RLT_ARC import RLT_ARC
from policy.LRU_block import LRU_block
from policy.mc_arc import MC_ARC
from policy.mc_arc_sans_rlt import MC_ARC_s
from policy.arcfilewithlifetime import ARC_File_Policyv2lifetime
from policy.BFH_ARC_whith_alpha_beta import BFH_ARC_alpha_beta
from policy.BFH_ARC_whithout_alpha_beta import BFH_ARC_whithout_alpha_beta
from policy.BFH_ARC_size_alfa import BFH_ARC_Size_alfa
from policy.wsm_rlt import BFH_wsm_rlt
from policy.rlt_position import rlt_position
from policy.cfs import CFS_ARC

# Global parameters
block_size = 4096  # Block size in bytes
alpha = 1.00

########################################
# IO Processing Functions
########################################

def process_io_request_with_queue(io_request, previous_end_time, policy,
                                  policy_hits_data, policy_misses_data,
                                  policy_evicted_fils_data, policy_evicted_blocks_data,
                                  policy_time_migration, policy_total_times,
                                  policy_prefetch_times, policy_read_times,
                                  policy_write_times, policy_nbr_of_blocks_hdd_reads):
    """
    Processes an IO request with an offset/block-size and updates policy statistics.
    
    :param io_request: The IORequest object to process.
    :param previous_end_time: The end time of the previous IO operation.
    :param policy: The caching policy instance.
    :param policy_hits_data: List to record hit counts.
    :param policy_misses_data: List to record miss counts.
    :param policy_evicted_fils_data: List to record evicted file counts.
    :param policy_evicted_blocks_data: List to record evicted block counts.
    :param policy_time_migration: List to record migration times.
    :param policy_total_times: List to record total times.
    :param policy_prefetch_times: List to record prefetch times.
    :param policy_read_times: List to record read times.
    :param policy_write_times: List to record write times.
    :param policy_nbr_of_blocks_hdd_reads: List to record number of HDD block reads.
    :return: The execution end time of the IO request.
    """
    io_request.execution_start_time = max(io_request.timestamp, previous_end_time)
    offset_block = math.floor(io_request.offsetStart / block_size)
    end_block = math.ceil(io_request.offsetEnd / block_size)

    policy.on_io(io_request.file, io_request.timestamp, io_request.requestType, offset_block, end_block)

    policy_hits_data.append(policy.hits)
    policy_misses_data.append(policy.misses)
    policy_evicted_fils_data.append(policy.evicted_file_count)
    policy_evicted_blocks_data.append(policy.evicted_blocks_count)
    policy_total_times.append(policy.total_time)
    policy_nbr_of_blocks_hdd_reads.append(policy.nbr_of_blocks_hdd_reads)

    io_request.execution_end_time = io_request.execution_start_time + policy.ssd_time
    return io_request.execution_end_time


def process_io_request_with_queue1(io_request, previous_end_time, policy,
                                   policy_hits_data, policy_misses_data,
                                   policy_evicted_fils_data, policy_evicted_blocks_data,
                                   policy_time_migration, policy_total_times,
                                   policy_prefetch_times, policy_read_times,
                                   policy_write_times, policy_nbr_of_blocks_hdd_reads):
    """
    Processes an IO request and updates a more comprehensive set of statistics.
    
    :param io_request: The IORequest object to process.
    :param previous_end_time: The end time of the previous IO operation.
    :param policy: The caching policy instance.
    :param policy_hits_data: List to record hit counts.
    :param policy_misses_data: List to record miss counts.
    :param policy_evicted_fils_data: List to record evicted file counts.
    :param policy_evicted_blocks_data: List to record evicted block counts.
    :param policy_time_migration: List to record migration times.
    :param policy_total_times: List to record total times.
    :param policy_prefetch_times: List to record prefetch times.
    :param policy_read_times: List to record read times.
    :param policy_write_times: List to record write times.
    :param policy_nbr_of_blocks_hdd_reads: List to record number of HDD block reads.
    :return: A tuple containing the execution end time and the processed IORequest.
    """
    io_request.execution_start_time = max(io_request.timestamp, previous_end_time)
    offset_block = math.floor(io_request.offsetStart / block_size)
    end_block = math.ceil(io_request.offsetEnd / block_size)

    policy.on_io(io_request.file, io_request.timestamp, io_request.requestType, offset_block, end_block)

    policy_hits_data.append(policy.hits)
    policy_misses_data.append(policy.misses)
    policy_evicted_fils_data.append(policy.evicted_file_count)
    policy_evicted_blocks_data.append(policy.evicted_blocks_count)
    policy_time_migration.append(policy.migration_times)
    policy_total_times.append(policy.total_time)
    policy_prefetch_times.append(policy.prefetch_times)
    policy_read_times.append(policy.read_times)
    policy_write_times.append(policy.write_times)
    policy_nbr_of_blocks_hdd_reads.append(policy.nbr_of_blocks_hdd_reads)

    io_request.execution_end_time = io_request.execution_start_time + policy.ssd_time

    return io_request.execution_end_time, io_request


def simulate_policy_with_queue1(policy, ios, policy_hits_data, policy_misses_data,
                                policy_evicted_fils_data, policy_evicted_blocks_data,
                                policy_time_migration, policy_total_times,
                                policy_prefetch_times, policy_read_times,
                                policy_write_times, policy_nbr_of_blocks_hdd_reads):
    """
    Simulates a replacement policy using a simple queue.
    
    :param policy: The caching policy instance to simulate.
    :param ios: A list of IO operations to process.
    :param policy_hits_data: List to record hit counts.
    :param policy_misses_data: List to record miss counts.
    :param policy_evicted_fils_data: List to record evicted file counts.
    :param policy_evicted_blocks_data: List to record evicted block counts.
    :param policy_time_migration: List to record migration times.
    :param policy_total_times: List to record total times.
    :param policy_prefetch_times: List to record prefetch times.
    :param policy_read_times: List to record read times.
    :param policy_write_times: List to record write times.
    :param policy_nbr_of_blocks_hdd_reads: List to record number of HDD block reads.
    :return: A list of processed IORequest objects.
    """
    io_queue = deque()
    previous_end_time = 0
    processed_io_requests = []

    for file, timestamp, requestType, offsetStart, offsetEnd in ios:
        io_request = IORequest(file, timestamp, requestType, offsetStart, offsetEnd)
        io_request.execution_start_time = max(io_request.timestamp, previous_end_time)

        if not io_queue or io_request.execution_start_time > previous_end_time:
            previous_end_time, processed_io = process_io_request_with_queue1(
                io_request, previous_end_time, policy,
                policy_hits_data, policy_misses_data,
                policy_evicted_fils_data, policy_evicted_blocks_data,
                policy_time_migration,
                policy_total_times,
                policy_prefetch_times,
                policy_read_times,
                policy_write_times,
                policy_nbr_of_blocks_hdd_reads
            )

            processed_io_requests.append(processed_io)
        else:
            io_queue.append(io_request)

    # Process remaining requests in the queue
    while io_queue:
        current_io = io_queue.popleft()
        waiting_time = current_io.waiting_time
        previous_end_time, processed_io = process_io_request_with_queue1(
            current_io, previous_end_time, policy,
            policy_hits_data, policy_misses_data,
            policy_evicted_fils_data, policy_evicted_blocks_data,
            policy_time_migration,
            policy_total_times,
            policy_prefetch_times,
            policy_read_times,
            policy_write_times,
            policy_nbr_of_blocks_hdd_reads
        )
        policy.total_time += waiting_time
        processed_io_requests.append(processed_io)

    return processed_io_requests


def simulate_policy_with_queue31(policy, ios, policy_hits_data, policy_misses_data,
                                 policy_evicted_fils_data, policy_evicted_blocks_data,
                                 policy_time_migration, policy_total_times,
                                 policy_prefetch_times, policy_read_times,
                                 policy_write_times, policy_nbr_of_blocks_hdd_reads):
    """
    Simulates a replacement policy with a queue and background eviction management.
    
    :param policy: The caching policy instance to simulate.
    :param ios: A list of IO operations to process.
    :param policy_hits_data: List to record hit counts.
    :param policy_misses_data: List to record miss counts.
    :param policy_evicted_fils_data: List to record evicted file counts.
    :param policy_evicted_blocks_data: List to record evicted block counts.
    :param policy_time_migration: List to record migration times.
    :param policy_total_times: List to record total times.
    :param policy_prefetch_times: List to record prefetch times.
    :param policy_read_times: List to record read times.
    :param policy_write_times: List to record write times.
    :param policy_nbr_of_blocks_hdd_reads: List to record number of HDD block reads.
    :return: A tuple containing aggregated simulation results.
    """
    io_queue = []
    previous_end_time = 0

    total_hits = 0
    total_misses = 0
    total_times_sum = 0
    migration_times = 0
    prefetch_times_sum = 0
    read_times_sum = 0
    write_times_sum = 0

    # Process IOs sequentially
    for file, timestamp, requestType, offsetStart, offsetEnd in ios:
        io_request = IORequest(file, timestamp, requestType, offsetStart, offsetEnd)
        io_request.execution_start_time = max(io_request.timestamp, previous_end_time)

        if not io_queue or io_request.execution_start_time > previous_end_time:
            previous_end_time = process_io_request_with_queue(
                io_request, previous_end_time, policy,
                policy_hits_data, policy_misses_data, policy_evicted_fils_data,
                policy_evicted_blocks_data, policy_time_migration,
                policy_total_times, policy_prefetch_times, policy_read_times,
                policy_write_times, policy_nbr_of_blocks_hdd_reads
            )

            total_times_sum += policy.total_time

            # Handle eviction if necessary
            while hasattr(policy, 'eviction_queue') and policy.eviction_queue and (not io_queue or io_request.execution_start_time > previous_end_time):
                policy.actual_evict()
                total_times_sum += policy.ssd_time_evict
        else:
            io_queue.append(io_request)

    # Process the remaining requests in the queue
    while io_queue:
        current_io = io_queue.pop(0)
        current_io.execution_start_time = max(current_io.timestamp, previous_end_time)
        waiting_time = current_io.waiting_time
        previous_end_time = process_io_request_with_queue(
            current_io, previous_end_time, policy,
            policy_hits_data, policy_misses_data, policy_evicted_fils_data,
            policy_evicted_blocks_data, policy_time_migration,
            policy_total_times, policy_prefetch_times, policy_read_times,
            policy_write_times, policy_nbr_of_blocks_hdd_reads
        )

        prefetch_times_sum += policy.prefetch_times
        read_times_sum += policy.read_times
        write_times_sum += policy.write_times
        total_times_sum += policy.total_time + waiting_time

    total_hits += policy.hits
    total_misses += policy.misses
    fils_migration = policy.evicted_file_count
    evicted_blocks_count = policy.evicted_blocks_count

    return (
        total_hits,
        total_misses,
        total_times_sum,
        migration_times,
        prefetch_times_sum,
        read_times_sum,
        write_times_sum,
        fils_migration,
        evicted_blocks_count,
        policy_nbr_of_blocks_hdd_reads
    )


########################################
# Global Simulation Functions
########################################

def simulate_all_policies(cache_size_proportions, ios, cache_size, ssd_tier, hdd_tier):
    """
    Simulates all caching policies for different cache size proportions and returns the results.
    
    :param cache_size_proportions: A list of cache size proportions to simulate.
    :param ios: A list of IO operations to process.
    :param cache_size: The base cache size.
    :param ssd_tier: The SSD tier instance.
    :param hdd_tier: The HDD tier instance.
    :return: A dictionary containing simulation results for each proportion and policy.
    """
    results = {}

    for proportion in cache_size_proportions:
        cache_size_p = round((cache_size * proportion) / 100)

        ssd_tier = Tier(
            "SSD",
            max_size=cache_size_p,
            latency=0.0001,
            read_throughput=2254857830,
            write_throughput=2147483648
        )
        # HDD tier remains unchanged according to the original code

        # Initialize the policies to test
        policies = {
            "ARC": Arc_block_Cache(cache_size_p, alpha, ssd_tier, hdd_tier, '', 0),
            "CFS-ARC": CFS_ARC(cache_size_p, alpha, ssd_tier, hdd_tier, '', 0),
            "FG-Arc": FG_ARC(cache_size_p, alpha, ssd_tier, hdd_tier, '', 0),
            "BFH_ARC_alpha_beta": BFH_ARC_alpha_beta(cache_size_p, alpha, ssd_tier, hdd_tier, '', 0),
            "BFH_ARC_without_alpha_beta": BFH_ARC_whithout_alpha_beta(cache_size_p, alpha, ssd_tier, hdd_tier, '', 0),
            "BFH_ARC_with_size": BFH_ARC(cache_size_p, alpha, ssd_tier, hdd_tier, '', 0),
            "RLT_ARC": RLT_ARC(cache_size_p, alpha, ssd_tier, hdd_tier, '', 0),
            "Idle_Time_BFH_ARC_policy": Idle_Time_BFH_ARC(cache_size_p, alpha, ssd_tier, hdd_tier, '', 0),
        }

        results[proportion] = {}
        for policy_name, policy in policies.items():
            hits_data, misses_data, total_times = [], [], []
            evicted_blocks_data, nbr_of_blocks_hdd_reads = [], []
            evicted_files_data = []

            if policy_name in ["Idle_Time_BFH_ARC_policy", "RLT_ARC", "CFS-ARC"]:
                # Case where simulate_policy_with_queue31 is used
                total_hits, total_misses, total_execution_time, migration_times, prefetch_time, read_time, write_time, \
                evicted_files, evicted_blocks, nbr_blocks_hdd = simulate_policy_with_queue31(
                    policy, ios, hits_data, misses_data, [], evicted_blocks_data, [],
                    total_times, [], [], [], []
                )
                hit_ratio = (total_hits / (total_hits + total_misses)) * 100 if (total_hits + total_misses) > 0 else 0
                results[proportion][policy_name] = {
                    "hit_ratio": hit_ratio,
                    "total_time": sum(total_times),
                    "evicted_blocks": evicted_blocks_data[-1] if evicted_blocks_data else 0,
                }
            else:
                # Use simulate_policy_with_queue1
                simulate_policy_with_queue1(
                    policy, ios, hits_data, misses_data, [], evicted_blocks_data, [],
                    total_times, [], [], [], []
                )
                hit_ratio = (hits_data[-1] / (hits_data[-1] + misses_data[-1])) * 100 if (hits_data[-1] + misses_data[-1]) > 0 else 0
                results[proportion][policy_name] = {
                    "hit_ratio": hit_ratio,
                    "total_time": sum(total_times),
                    "evicted_blocks": evicted_blocks_data[-1] if evicted_blocks_data else 0,
                }
    return results


def plot_results(results, metric='evicted_blocks', save_path='graphs'):
    """
    Plots the simulation results based on a given metric.
    
    :param results: The simulation results dictionary.
    :param metric: The metric to plot ('evicted_blocks', 'hit_ratio', 'total_time').
    :param save_path: The directory path where the graph will be saved.
    """
    proportions = sorted(results.keys())
    policies = sorted(next(iter(results.values())).keys())

    fig, ax = plt.subplots(figsize=(12, 8))
    width = 0.35
    ind = np.arange(len(proportions))
    offset = width / len(policies)

    for i, policy in enumerate(policies):
        values = [results[proportion][policy][metric] for proportion in proportions]
        ax.bar(ind + i * offset, values, width=offset, label=policy)

    ax.set_xlabel('Cache Size Proportion (%)')
    ax.set_title(f'{metric.replace("_", " ").title()} by Policy and Cache Proportion')
    ax.set_xticks(ind + offset / 2 * (len(policies) - 1))
    ax.set_xticklabels(proportions)
    ax.legend()

    if metric == 'hit_ratio':
        ax.set_ylabel('Hit Ratio (%)')
    elif metric == 'total_time':
        ax.set_ylabel('Total Time')
    elif metric == 'evicted_blocks':
        ax.set_ylabel('Number of Evicted Blocks')

    plt.tight_layout()
    current_datetime = datetime.datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"{metric}_{formatted_datetime}.png"
    filepath = os.path.join(save_path, filename)
    plt.savefig(filepath)
    plt.close(fig)


def write_summary_to_file2(log_filename, metadata_filename, cache_size_p, proportion, hits, misses,
                           total_execution_time, total_evicted_files, total_evicted_blocks,
                           nbr_of_blocks_hdd_reads, IO_count):
    """
    Writes a simplified summary of the results to a file.
    
    :param log_filename: The filename to write the summary to.
    :param metadata_filename: The metadata filename.
    :param cache_size_p: The proportional cache size.
    :param proportion: The cache size proportion.
    :param hits: Number of hits.
    :param misses: Number of misses.
    :param total_execution_time: Total execution time.
    :param total_evicted_files: Total number of evicted files.
    :param total_evicted_blocks: Total number of evicted blocks.
    :param nbr_of_blocks_hdd_reads: Number of HDD block reads.
    :param IO_count: Total number of IO operations.
    """
    with open(log_filename, 'w') as file:
        file.write(f"Metadata File: {metadata_filename}\n")
        file.write(f"Cache Size: {cache_size_p}\n")
        file.write(f"Cache Proportion: {proportion}%\n")
        file.write(f"Hit Ratio: {((hits / (misses + hits)) * 100):.2f}%\n")
        file.write(f"Hits: {hits}\n")
        file.write(f"Misses: {misses}\n")
        file.write(f"Miss Ratio: {((misses / (misses + hits)) * 100):.2f}%\n")
        file.write(f"Total Execution Time: {total_execution_time}\n")
        file.write(f"Total Evicted Blocks: {total_evicted_blocks}\n")
        file.write(f"Total Evicted Files: {total_evicted_files}\n")
        file.write(f"Total Blocks HDD Reads: {nbr_of_blocks_hdd_reads}\n")
        file.write(f"Total IOs: {IO_count}\n")


def write_summary_to_file(log_filename, metadata_filename, cache_size_p, hits, misses,
                          total_execution_time, total_eviction_time, total_prefetch_time,
                          total_read_time, total_write_time, total_fils_migrated,
                          total_blocks_migrated, nbr_of_blocks_hdd_reads, IO_count, eviction_misses):
    """
    Writes a more comprehensive summary of the results to a file.
    
    :param log_filename: The filename to write the summary to.
    :param metadata_filename: The metadata filename.
    :param cache_size_p: The proportional cache size.
    :param hits: Number of hits.
    :param misses: Number of misses.
    :param total_execution_time: Total execution time.
    :param total_eviction_time: Total eviction time.
    :param total_prefetch_time: Total prefetch time.
    :param total_read_time: Total read time.
    :param total_write_time: Total write time.
    :param total_fils_migrated: Total number of migrated files.
    :param total_blocks_migrated: Total number of migrated blocks.
    :param nbr_of_blocks_hdd_reads: Number of HDD block reads.
    :param IO_count: Total number of IO operations.
    :param eviction_misses: Number of eviction misses.
    """
    with open(log_filename, 'w') as file:
        hit_ratio = (hits / (misses + hits)) * 100 if (misses + hits) > 0 else 0
        miss_ratio = (misses / (misses + hits)) * 100 if (misses + hits) > 0 else 0
        eviction_miss_percentage = (eviction_misses / misses) * 100 if misses > 0 else 0

        file.write(f"Metadata File: {metadata_filename}\n")
        file.write(f"Cache Size: {cache_size_p}\n")
        file.write(f"Hit Ratio: {hit_ratio:.2f}%\n")
        file.write(f"Miss Ratio: {miss_ratio:.2f}%\n")
        file.write(f"Eviction Miss Percentage: {eviction_miss_percentage:.2f}%\n")
        file.write(f"Hits: {hits}\n")
        file.write(f"Misses: {misses}\n")
        file.write(f"Total Execution Time: {total_execution_time}\n")
        file.write(f"Total Eviction Time: {total_eviction_time}\n")
        file.write(f"Total Prefetch Time: {total_prefetch_time}\n")
        file.write(f"Total Read Time: {total_read_time}\n")
        file.write(f"Total Write Time: {total_write_time}\n")
        file.write(f"Total Evicted Blocks: {total_blocks_migrated}\n")
        file.write(f"Total Evicted Files: {total_fils_migrated}\n")
        file.write(f"Total Blocks HDD Reads: {nbr_of_blocks_hdd_reads}\n")
        file.write(f"Total IOs: {IO_count}\n")


def read_io_requests(filename):
    """
    Reads IO requests from a CSV file.
    
    :param filename: The path to the IO requests CSV file.
    :return: A generator yielding tuples of (File, timestamp, requestType, offsetStart, offsetEnd).
    """
    with open(filename, 'r', encoding='utf-8') as f:
        next(f)  # Skip header line
        for line in f:
            row = line.strip().split(',')
            filename = row[2]
            timestamp = float(row[0])
            requestType = str(row[1])
            offsetStart = int(row[3])
            offsetEnd = int(row[4])
            yield (files_dict.get(filename, None), timestamp, requestType, offsetStart, offsetEnd)


def read_io_requests1(filename, limit_number):
    """
    Reads a limited number of IO requests from a CSV file.
    
    :param filename: The path to the IO requests CSV file.
    :param limit_number: The maximum number of IO requests to read.
    :return: A generator yielding tuples of (File, timestamp, requestType, offsetStart, offsetEnd).
    """
    with open(filename, 'r', encoding='utf-8') as f:
        next(f)  # Skip header line
        for i, line in enumerate(f):
            if i >= limit_number:
                break
            row = line.strip().split(',')
            filename = row[2]
            timestamp = float(row[0])
            requestType = str(row[1])
            offsetStart = int(row[3])
            offsetEnd = int(row[4])
            yield (files_dict.get(filename, None), timestamp, requestType, offsetStart, offsetEnd)


def load_file_metadata(metadata_filename, block_size):
    """
    Loads file metadata and creates File objects.
    
    :param metadata_filename: The path to the metadata CSV file.
    :param block_size: The size of each block in bytes.
    :return: A dictionary mapping filenames to File objects.
    """
    files_dict = {}
    with open(metadata_filename, 'r', encoding='utf-8') as f:
        next(f)  # Skip header line
        for line in f:
            row = line.strip().split(',')
            filename = row[0]
            size = int(row[1])
            firstAccessTime = float(row[2])
            lastAccessTime = float(row[3])
            lifetime = float(row[4])
            files_dict[filename] = File(
                filename,
                max(1, math.ceil(size / block_size)),
                firstAccessTime,
                lastAccessTime,
                lifetime
            )
    return files_dict


def load_file_metadata_user(metadata_filename, io_filename, cache_size, num_users, max_lines):
    """
    Loads file metadata and distributes files among users based on their IO load.
    
    :param metadata_filename: The path to the metadata CSV file.
    :param io_filename: The path to the IO requests CSV file.
    :param cache_size: The total cache size.
    :param num_users: The number of users.
    :param max_lines: The maximum number of IO requests to process.
    :return: A tuple containing the files dictionary and users dictionary.
    """
    from collections import defaultdict
    users = {}
    files_dict = {}

    space_default = round(cache_size / num_users)
    for user_id in range(1, num_users + 1):
        users[user_id] = User(user_id, space_default)

    total_io_bytes_accessed_per_file = defaultdict(int)
    with open(io_filename, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        for i, row in enumerate(reader):
            if i >= max_lines:
                break
            timestamp = float(row[0])
            requestType = str(row[1])
            filename = row[2]
            offsetStart = int(row[3])
            offsetEnd = int(row[4])
            io_size = offsetEnd - offsetStart
            total_io_bytes_accessed_per_file[filename] += io_size

    file_metadata = {}
    with open(metadata_filename, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        for row in reader:
            filename = row[0]
            size = int(row[1])
            firstAccessTime = float(row[2])
            lastAccessTime = float(row[3])
            lifetime = float(row[4])
            file_metadata[filename] = (size, firstAccessTime, lastAccessTime, lifetime)

    files_to_assign = {}
    for filename in total_io_bytes_accessed_per_file:
        if filename in file_metadata:
            size, firstAccessTime, lastAccessTime, lifetime = file_metadata[filename]
            total_bytes_accessed = total_io_bytes_accessed_per_file[filename]
            files_to_assign[filename] = {
                'size': size,
                'firstAccessTime': firstAccessTime,
                'lastAccessTime': lastAccessTime,
                'lifetime': lifetime,
                'total_bytes_accessed': total_bytes_accessed
            }

    user_total_bytes_accessed = {user_id: 0 for user_id in users}
    user_total_file_size = {user_id: 0 for user_id in users}

    sorted_files = sorted(files_to_assign.items(), key=lambda item: item[1]['total_bytes_accessed'], reverse=True)

    for filename, file_info in sorted_files:
        min_user_id = min(user_total_bytes_accessed, key=user_total_bytes_accessed.get)
        user_total_bytes_accessed[min_user_id] += file_info['total_bytes_accessed']
        user_total_file_size[min_user_id] += file_info['size']
        files_dict[filename] = File(
            filename,
            max(1, math.ceil(file_info['size'] / 4096)),
            min_user_id,
            file_info['firstAccessTime'],
            file_info['lastAccessTime'],
            file_info['lifetime']
        )

    return files_dict, users


########################################
# Main Simulation
########################################

if __name__ == "__main__":
    # Simulation parameters
    cache_size_proportions = [0.5, 1, 5, 10, 15]
    i = 3

    for proportion in cache_size_proportions:
        log_file_path = f"app/yombo/result/20/lfu/lfu_suarcmmardataset_{i}_.txt"
        cache_size = round(335798100142 / block_size)  

        max_lines = 1000000000
        """
        metadata_filename = "app/google/google1_metadata1.csv"  # Metadata file

        ios_requests = 'app/google/google1_request1.csv'
        """
        
        metadata_filename = 'data3/IBMObjectStoreTrace003Part0_metadata.csv'
        ios_requests = 'data3/IBMObjectStoreTrace003Part0_request.csv'
        

        """metadata_filename = "app/yombo/yombo_metadata1_update.csv"  # Metadata file
        ios_requests = 'app/yombo/yombo_request11.csv'"""

        files_dict, users = load_file_metadata_user(
            metadata_filename, ios_requests,
            round((proportion * cache_size) / 100),
            num_users=20,
            max_lines=max_lines
        )

        ios = list(read_io_requests1('data3/IBMObjectStoreTrace003Part0_request.csv', max_lines))
        cache_size_p = round((cache_size * proportion) / 100)
        IO_count = len(ios)
        print("IO_count", IO_count)

        ssd_tier = Tier(
            "SSD",
            max_size=cache_size_p,
            latency=0.0001,
            read_throughput=2254857830,
            write_throughput=2147483648
        )
        hdd_tier = Tier(
            "HDD",
            max_size=cache_size,
            latency=0.01,
            read_throughput=262144000,
            write_throughput=251658240
        )

        hits_data1 = []
        misses_data1 = []
        evicted_blocks_data1 = []
        migration_time1 = []
        total_times = []
        prefetch_times = []
        read_times = []
        write_times = []
        nbr_of_blocks_hdd_readss = []
        evicted_files_data1 = []

        # Selected policies: BFH_wsm1_mc_arc (for example)
        BFH_wsm1_mc_arc_policy = BFH_wsm1_mc_arc(
            cache_size_p, users, alpha, ssd_tier, hdd_tier, log_file_path, IO_count
        )
        mc_arc_bb_policy = mc_arc_bb(
            cache_size_p, users, alpha, ssd_tier, hdd_tier, log_file_path, IO_count
        )  # mc_arc_bb
        arc_blocs_bb_policy = Arc_block_Cache(
            cache_size_p, users, alpha, ssd_tier, hdd_tier, log_file_path, IO_count
        )  # arc_file
        LRU_block_bb_policy = LRU_block_bb(
            cache_size_p, users, alpha, ssd_tier, hdd_tier, log_file_path, IO_count
        )  # lru_bb
        LFU_block_bb_policy = LFU_bb_l(
            cache_size_p, users, alpha, ssd_tier, hdd_tier, log_file_path, IO_count
        )  # lfu_bb_l
        BFH_topsis_bb_policy = BFH_topsis_bb(
            cache_size_p, users, alpha, ssd_tier, hdd_tier, log_file_path, IO_count
        )  # topsis

        io_requests_list = simulate_policy_with_queue1(
            LFU_block_bb_policy, ios, hits_data1, misses_data1,
            evicted_files_data1, evicted_blocks_data1,
            migration_time1, total_times, prefetch_times,
            read_times, write_times, nbr_of_blocks_hdd_readss
        )

        current_datetime = datetime.datetime.now()
        formatted_datetime = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")
        log_filename = f"app/yombo/result/20/lfu/lfu_suarcmmardataset_{i}_{formatted_datetime}.txt"
        i += 1

        write_summary_to_file(
            log_filename, metadata_filename, cache_size_p,
            hits_data1[-1], misses_data1[-1],
            sum(total_times), sum(migration_time1), sum(prefetch_times),
            sum(read_times), sum(write_times), evicted_files_data1[-1],
            evicted_blocks_data1[-1], nbr_of_blocks_hdd_readss[-1],
            IO_count, mc_arc_bb_policy.eviction_misses
        )
