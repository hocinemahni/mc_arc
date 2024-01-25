import datetime
from structures.io import IORequest
from collections import deque
from structures.tier import Tier
from structures.file import File
from policy.Idle_Time_BFH_ARC import Idle_Time_BFH_ARC
from policy.arcblock import Arc_block_Cache
from policy.FG_ARC import FG_ARC
#from policy.arcv1 import ARC_File_Policyv1
from policy.BFH_ARC import BFH_ARC
from policy.RLT_ARC import RLT_ARC
from policy.arcfilewithlifetime import ARC_File_Policyv2lifetime
import math


def process_io_request_with_queue(io_request, previous_end_time, policy, policy_hits_data, policy_misses_data,policy_evicted_fils_data,
                                  policy_evicted_blocks_data, policy_time_migration, policy_total_times, policy_prefetch_times, policy_read_times, policy_write_times):
    """ Traite une demande IO """
    io_request.execution_start_time = max(io_request.timestamp, previous_end_time)

    offset_block = math.floor(io_request.offsetStart / block_size)
    end_block = math.ceil(io_request.offsetEnd / block_size) 
    #size_block = end_block - offset_block

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
    io_request.execution_end_time = io_request.execution_start_time + policy.total_time
    return io_request.execution_end_time


def process_io_request_with_queue1(io_request, previous_end_time, policy, policy_hits_data, policy_misses_data, policy_evicted_fils_data,
                                  policy_evicted_blocks_data, policy_time_migration, policy_total_times, policy_prefetch_times, policy_read_times, policy_write_times):
    """ Traite une demande IO """
    io_request.execution_start_time = max(io_request.timestamp, previous_end_time)

    offset_block = math.floor(io_request.offsetStart / block_size)
    end_block = math.ceil(io_request.offsetEnd / block_size)
    #size_block = end_block - offset_block
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
    io_request.execution_end_time = io_request.execution_start_time + policy.total_time
    return io_request.execution_end_time, io_request


def simulate_policy_with_queue1(policy, ios, policy_hits_data, policy_misses_data,policy_evicted_fils_data, policy_evicted_blocks_data, policy_time_migration, policy_total_times, policy_prefetch_times, policy_read_times, policy_write_times):
    io_queue = deque()
    previous_end_time = 0
    processed_io_requests = []

    for file, timestamp, requestType, offsetStart, offsetEnd in ios:
        io_request = IORequest(file, timestamp, requestType, offsetStart, offsetEnd)
        io_request.execution_start_time = max(io_request.timestamp, previous_end_time)
        if not io_queue or io_request.execution_start_time > previous_end_time:

            previous_end_time, processed_io = process_io_request_with_queue1(io_request, previous_end_time, policy, policy_hits_data, policy_misses_data,policy_evicted_fils_data, policy_evicted_blocks_data, policy_time_migration, policy_total_times, policy_prefetch_times, policy_read_times, policy_write_times)

            processed_io_requests.append(processed_io)
        else:
            io_queue.append(io_request)

    while io_queue:
        current_io = io_queue.popleft()
        waiting_time = current_io.waiting_time
        previous_end_time, processed_io = process_io_request_with_queue1(current_io, previous_end_time, policy, policy_hits_data, policy_misses_data,policy_evicted_fils_data, policy_evicted_blocks_data, policy_time_migration, policy_total_times, policy_prefetch_times, policy_read_times, policy_write_times)
        #processed_io_requests.append(processed_io)
        policy.total_time += waiting_time
        processed_io_requests.append(processed_io)
    return processed_io_requests


def simulate_policy_with_queue31(policy, ios,
                                 policy_hits_data, policy_misses_data,policy_evicted_fils_data,
                                 policy_evicted_blocks_data, policy_time_migration,
                                 policy_total_times, policy_prefetch_times, policy_read_times, policy_write_times ):
    io_queue = []
    previous_end_time = 0

    # Initialisation des compteurs pour les hits, les misses et le temps total
    total_hits = 0
    total_misses = 0
    total_times = 0
    migration_times = 0
    prefetch_times = 0
    read_times = 0
    write_times = 0
    for file, timestamp, requestType, offsetStart, offsetEnd in ios:
        io_request = IORequest(file, timestamp, requestType, offsetStart, offsetEnd)
        io_request.execution_start_time = max(io_request.timestamp, previous_end_time)
        if not io_queue or io_request.execution_start_time > previous_end_time:
            previous_end_time = process_io_request_with_queue(
                io_request, previous_end_time, policy,
                policy_hits_data, policy_misses_data,policy_evicted_fils_data,
                policy_evicted_blocks_data, policy_time_migration,
                policy_total_times, policy_prefetch_times, policy_read_times, policy_write_times)
            total_times += policy.total_time
            # Traitement de l'éviction et ajout du temps de migration au temps total
            while policy.eviction_queue and (not io_queue or io_request.execution_start_time > previous_end_time):
                policy.actual_evict()
                total_times += policy.ssd_time_evict

        else:
            io_queue.append(io_request)

    while io_queue:

        current_io = io_queue.pop(0)
        current_io.execution_start_time = max(current_io.timestamp, previous_end_time)
        waiting_time = current_io.waiting_time
        previous_end_time = process_io_request_with_queue(
            current_io, previous_end_time, policy,
            policy_hits_data, policy_misses_data,policy_evicted_fils_data,
            policy_evicted_blocks_data, policy_time_migration,
            policy_total_times, policy_prefetch_times, policy_read_times, policy_write_times)
        prefetch_times += policy.prefetch_times
        read_times += policy.read_times
        write_times += policy.write_times
        total_times += policy.total_time + waiting_time
    # Mise à jour des compteurs
    total_hits += policy.hits
    total_misses += policy.misses
    fils_migration = policy.evicted_file_count
    evicted_blocks_count = policy.evicted_blocks_count

    # Retour des totaux accumulés
    return total_hits, total_misses, total_times, migration_times, prefetch_times, read_times, write_times, fils_migration, evicted_blocks_count


def write_summary_to_file(log_filename, metadata_filename, cache_size_p, hits, misses, total_execution_time, total_eviction_time, total_prefetch_time, total_read_time, total_write_time, total_fils_migrated,total_blocks_migrated, IO_count):
    with open(log_filename, 'w') as file:
        file.write(f"Metadata File: {metadata_filename}\n")
        file.write(f"Cache Size: {cache_size_p}\n")
        file.write(f"La proportion du cache: {proportion}\n")
        file.write(f" ritio Hits: {((hits / (misses + hits)) * 100)}\n")
        file.write(f"Hits: {hits }\n")
        file.write(f"misses: {misses }\n")
        file.write(f" ratioMisses: {((misses / (misses+hits)) *100) }\n")
        file.write(f"Total Execution Time: {total_execution_time}\n")
        file.write(f"Total Eviction Time: {total_eviction_time}\n")
        file.write(f"Total Prefetch Time: {total_prefetch_time}\n")
        file.write(f"Total Read Time: {total_read_time}\n")
        file.write(f"Total Write Time: {total_write_time}\n")
        file.write(f"Total Evicted Blocks: {total_blocks_migrated}\n")
        file.write(f"Total Evicted Files: {total_fils_migrated}\n")
        file.write(f"Total IOs: {IO_count}\n")
        

def read_io_requests(filename):
     with open(filename, 'r', encoding='utf-8') as f:
         next(f)  # skip header line
         for line in f:
             row = line.strip().split(',')
             filename = row[2]
             timestamp = float(row[0])
             requestType = str(row[1])
             offsetStart = int(row[3])
             offsetEnd = int(row[4])
             yield (files_dict.get(filename, None), timestamp, requestType, offsetStart, offsetEnd)
def read_io_requests1(filename, limit_number):
    with open(filename, 'r', encoding='utf-8') as f:
        next(f)  # skip header line
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


if __name__ == "__main__":

    block_size = 1024
    alpha = 1.00
    #cache_size_proportion = [0.1, 0.5, 1, 5]
    cache_size_proportion = [20]
    files_dict = {}
    metadata_filename = "data3/IBMObjectStoreTrace000Part0_metadata.csv"
    with open(metadata_filename, 'r', encoding='utf-8') as f:
        next(f)
        for line in f:
            row = line.strip().split(',')
            filename = row[0]
            size = int(row[1])
            firstAccessTime = float(row[2])
            lastAccessTime = (row[3])
            lifetime = float(row[4])
            files_dict[filename] = File(filename,  max(1, math.ceil(size / block_size)), firstAccessTime, lastAccessTime, lifetime)
    #ios = list(read_io_requests("data3/IBMObjectStoreTrace010Part0_request.csv"))
    ios = list(read_io_requests1("data3/IBMObjectStoreTrace000Part0_request.csv", 1000))
    IO_count = len(ios)
    i = 10
    for proportion in cache_size_proportion:
        cache_size = round(2366972196 / block_size)  #476094935 w7
        cache_size_p = round((cache_size * proportion)/100)
        ssd_tier = Tier("SSD", max_size=cache_size_p, latency=0.0001, read_throughput=2254857830, write_throughput=2147483648)
        hdd_tier = Tier("HDD", max_size=cache_size, latency=0.01, read_throughput=262144000, write_throughput=251658240)
        hits_data1 = []
        misses_data1 = []
        evicted_blocks_data1 = []
        migration_time1 = []
        total_times1 = []
        migration_time_evict1 = []
        total_times = []
        prefetch_times = []
        read_times = []
        write_times = []
        evicted_files_data1 = []
        RLT_ARC_policy = RLT_ARC(cache_size_p, alpha, ssd_tier, hdd_tier)
        arc_file_lifetime = ARC_File_Policyv2lifetime(cache_size_p, alpha, ssd_tier, hdd_tier)
        BFH_ARC_policy = BFH_ARC(cache_size_p, alpha, ssd_tier, hdd_tier)
        FG_ARC_policy = FG_ARC(cache_size_p, alpha, ssd_tier, hdd_tier)
        arc_block_policy = Arc_block_Cache(cache_size_p, alpha, ssd_tier, hdd_tier)
        Idle_Time_BFH_ARC = Idle_Time_BFH_ARC(cache_size_p, alpha, ssd_tier, hdd_tier)
        '''total_hits, total_misses, total_execution_time, migration_times, prefetch_time, read_time, write_time, evicted_files, evicted_blocks= simulate_policy_with_queue31(arc_file_policy2_evict, ios, hits_data1, misses_data1,evicted_files_data1,
                                                        evicted_blocks_data1, migration_time1, total_times, prefetch_times, read_times, write_times)
        print(f'total_hits {total_hits}')
        print(f' total_miss {total_misses}')
        print(f' total_time {total_execution_time}')
        print(f'migration time{migration_times}')'''
        # sum_of_total_times1 = sum(total_times)

        io_requests_list = simulate_policy_with_queue1(arc_block_policy, ios, hits_data1, misses_data1,evicted_files_data1,
                                                       evicted_blocks_data1, migration_time1, total_times, prefetch_times, read_times, write_times)
        current_datetime = datetime.datetime.now()

        # Formater la date et l'heure en une chaîne ("2023-04-01_12-30-00")
        formatted_datetime = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")

        # Créer le nom du fichier avec la date et l'heure
        log_filename = f"workloadnew2/workload1/arcblock/summardataset_{i}_{formatted_datetime}.txt"
        i += 1
        write_summary_to_file(log_filename, metadata_filename, cache_size_p, hits_data1[-1], misses_data1[-1], sum(total_times), sum(migration_time1), sum(prefetch_times), sum(read_times), sum(write_times), evicted_files_data1[-1], evicted_blocks_data1[-1], IO_count)
        #write_summary_to_file(log_filename, metadata_filename, cache_size_p, total_hits, total_misses, total_execution_time, migration_times, prefetch_time, read_time, write_time,evicted_files,evicted_blocks,  IO_count)