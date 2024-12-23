# -*- coding: utf-8 -*-
from collections import defaultdict, deque, OrderedDict
from structures.file import File
from policy.policy import Policy
import numpy as np
import datetime

class mc_arc_topsis(Policy):
    """
    MC-ARC is a multi-criteria, file-level eviction policy
    that combines frequency and recency of access with file
    lifetime and fairness considerations.
    """

    def __init__(self, cache_size, users, alpha, ssd_tier, hdd_tier, log_file_path, total_io_count):
        """
        Initialize the BFH_topsis_bb caching policy instance.

        :param cache_size: The size of the cache (in number of blocks).
        :param users: A dictionary of user objects.
        :param alpha: A parameter used by the policy for adaptation.
        :param ssd_tier: The SSD tier, managing its own read/write operations.
        :param hdd_tier: The HDD tier, managing its own read/write operations.
        :param log_file_path: Path to the log file where user times are logged.
        :param total_io_count: Total number of IO operations.
        """
        super().__init__(cache_size, alpha, ssd_tier, hdd_tier)
        
        # Indicates if we are dealing with a new file
        self.new_file = False

        # p: adaptation parameter that changes with the policy
        self.p = 0

        # Number of prefetch times (not used in detail here)
        self.prefetch_times = 0

        # Counter for IO operations
        self.io_counter = 0

        # Total IO operations that will be handled by the system
        self.total_io_operations = total_io_count

        # Dictionary of users
        self.users = users

        # c: the cache size (in blocks)
        self.c = cache_size

        # alpha: adaptation parameter from the policy
        self.alpha = alpha

        # Block size = 4 KB
        self.block_size = 4 * 1024

        # SSD and HDD tiers
        self.ssd_tier = ssd_tier
        self.hdd_tier = hdd_tier

        # Path to the log file for user time logging
        self.log_file_path = log_file_path

        # Main cache structures: T1, T2 for actual data; B1, B2 for ghost entries
        self.t1 = OrderedDict()
        self.t2 = OrderedDict()
        self.b1 = OrderedDict()
        self.b2 = OrderedDict()

        # Accumulator for output messages (if needed)
        self.output_accumulator = ""

        # Count of blocks read from HDD
        self.nbr_of_blocks_hdd_reads = 0

        # Hits and Misses
        self.hits = 0
        self.misses = 0

        # Eviction misses (file was evicted from SSD) and cold misses (file never in SSD before)
        self.eviction_misses = 0
        self.cold_misses = 0

        # Track user requests and throughput metrics
        self.user_requests = defaultdict(int)
        self.user_request_sizes = defaultdict(int)
        self.user_total_time = defaultdict(float)

        # Counters for read/write times and total time
        self.read_times = 0
        self.write_times = 0
        self.total_time = 0

        # Eviction metrics
        self.evicted_blocks_count = 0
        self.evicted_file_count = 0
        self.file2blocks = defaultdict(set)
        self.file2tier = defaultdict(int)
        self.migration_times = 0
        self.adapte_B1 = 0
        self.adapte_B2 = 0

        # Time spent accessing SSD and HDD
        self.ssd_time = 0
        self.hdd_time = 0

        # Track which files are currently in SSD and which have been evicted from SSD
        self.files_in_ssd = set()
        self.evicted_files_SSD = set()

        # Path to eviction log file
        self.eviction_log_file = 'app/fake/scen1/20/wsm/eviction_log.txt'

        # Track all evicted files
        self.evicted_files = set()

        # Size of the "burst buffer" = 256 KB
        self.burst_buffer_size = 256 * 1024

    def t1_max_size(self) -> int:
        """
        Get the maximum size for T1 based on the current adaptation parameter p.

        :return: Maximum size for T1.
        """
        return self.p

    def t2_max_size(self) -> int:
        """
        Get the maximum size for T2 based on the current cache size and adaptation parameter p.

        :return: Maximum size for T2.
        """
        return self.c - self.p

    def b1_max_size(self) -> int:
        """
        Get the maximum size for B1 based on the current cache size and adaptation parameter p.

        :return: Maximum size for B1.
        """
        return self.c - self.p

    def b2_max_size(self) -> int:
        """
        Get the maximum size for B2 based on the current adaptation parameter p.

        :return: Maximum size for B2.
        """
        return self.p

    def evict(self):
        """
        Evict a file from SSD to HDD using the TOPSIS method.
        The file with the highest TOPSIS score is selected for eviction.
        """
        criteria_scores = self.calculate_criteria_scores()
        normalized_scores = self.normalize_scores(criteria_scores)
        topsis_scores = self.calculate_topsis_scores(normalized_scores)
        file_to_evict = max(topsis_scores, key=topsis_scores.get)
        self.evict_file(file_to_evict)

    def calculate_criteria_scores(self):
        """
        Calculate the scores for each file based on position, lifetime, and fairness.

        :return: A dictionary mapping each file to its criteria scores as a numpy array.
        """
        file2position_score = defaultdict(float)
        file2lifetime_score = defaultdict(float)
        file2fairness_score = defaultdict(float)

        # Calculate position-based scores
        for i, (file, _) in enumerate(self.t1):
            file2position_score[file] += len(self.t1) - i

        for j, (file, _) in enumerate(self.t2):
            file2position_score[file] += len(self.t2) - j

        # Normalize position scores by file size
        for file in file2position_score.keys():
            file2position_score[file] /= file.size

        # Calculate lifetime and fairness scores
        for file in self.file2blocks:
            time_diff = (file.lifetime - (self.time_now - file.firstAccessTime)) / 1000
            file2lifetime_score[file] = np.exp(-np.float64(time_diff))
            if self.users[file.user_id].space_default > 0:
                file2fairness_score[file] = (self.users[file.user_id].space_utilization /
                                             self.users[file.user_id].space_default)

        # Create a dictionary of combined scores
        scores = {file: np.array([file2position_score[file],
                                  file2lifetime_score[file],
                                  file2fairness_score[file]]) for file in self.file2blocks}
        return scores

    def normalize_scores(self, scores):
        """
        Normalize the criteria scores using vector normalization.

        :param scores: A dictionary mapping files to their criteria scores.
        :return: The normalized scores as a dictionary.
        """
        norms = np.linalg.norm(list(scores.values()), axis=0)
        for file in scores:
            scores[file] /= norms
        return scores

    def calculate_topsis_scores(self, scores):
        """
        Calculate the TOPSIS scores for each file based on normalized criteria scores.

        :param scores: A dictionary mapping files to their normalized criteria scores.
        :return: A dictionary mapping files to their TOPSIS scores.
        """
        weights = np.array([1 / 3, 1 / 3, 1 / 3])  # Equal weights for all criteria
        weighted_scores = {file: score * weights for file, score in scores.items()}
        ideal_best = np.max(list(weighted_scores.values()), axis=0)
        ideal_worst = np.min(list(weighted_scores.values()), axis=0)

        topsis_scores = {}
        for file, score in weighted_scores.items():
            distance_to_best = np.linalg.norm(score - ideal_best)
            distance_to_worst = np.linalg.norm(score - ideal_worst)
            topsis_scores[file] = np.float64(distance_to_worst) / (distance_to_best + distance_to_worst)
        return topsis_scores

    def evict_file(self, worse_file):
        """
        Evict the specified file from SSD to HDD and update relevant metrics.

        :param worse_file: The File object to evict.
        """
        # Decrease the user's SSD space usage
        self.users[worse_file.user_id].decrease_space(worse_file.size)

        # Remove the file from the SSD tier and add it to the HDD tier
        self.ssd_tier.remove_file(worse_file.name)
        self.hdd_tier.add_file(worse_file)

        # Update eviction counters
        self.evicted_blocks_count += worse_file.size
        self.evicted_file_count += 1

        # Remove all blocks of the file from cache structures
        self.remove_all(worse_file)

        # Add the evicted file to the list for burst transfer
        if not hasattr(self, 'evicted_files_during_request'):
            self.evicted_files_during_request = []
        self.evicted_files_during_request.append(worse_file)

        # Manage ghost lists B1 and B2 based on cache size constraints
        if (len(self.t1) + len(self.b1)) == self.c:
            if len(self.t1) < self.c:
                if len(self.b1) >= worse_file.size:
                    for _ in range(worse_file.size):
                        oldest_key2 = next(iter(self.b1))  # Get the first key from the dictionary
                        self.b1.pop(oldest_key2)
                else:
                    if len(self.b1) >= self.adapte_B1:
                        for _ in range(self.adapte_B1):
                            oldest_key = next(iter(self.b1))  # Get the first key from the dictionary
                            self.b1.pop(oldest_key)
                    else:
                        nombre_blocs_supprimes_b1 = len(self.b1)
                        self.b1.clear()
                        for _ in range(worse_file.size - nombre_blocs_supprimes_b1):
                            oldest_key2 = next(iter(self.b2))  # Get the first key from the dictionary
                            self.b2.pop(oldest_key2)
                    if len(self.b2) >= self.adapte_B2:
                        for _ in range(self.adapte_B2):
                            oldest_key2 = next(iter(self.b2))  # Get the first key from the dictionary
                            self.b2.pop(oldest_key2)
                    else:
                        nombre_blocs_supprimes_b2 = len(self.b2)
                        self.b2.clear()
                        for _ in range(worse_file.size - nombre_blocs_supprimes_b2):
                            oldest_key2 = next(iter(self.b1))  # Get the first key from the dictionary
                            self.b1.pop(oldest_key2)
        elif (len(self.t1) + len(self.t2) + len(self.b1) + len(self.b2)) >= self.c:
            if (len(self.t1) + len(self.t2) + len(self.b1) + len(self.b2)) == (self.c * 2):
                if len(self.b2) >= worse_file.size:
                    for _ in range(worse_file.size):
                        oldest_key2 = next(iter(self.b2))  # Get the first key from the dictionary
                        self.b2.pop(oldest_key2)
                else:
                    if len(self.b2) >= self.adapte_B2:
                        for _ in range(self.adapte_B2):
                            oldest_key2 = next(iter(self.b2))  # Get the first key from the dictionary
                            self.b2.pop(oldest_key2)
                    else:
                        nombre_blocs_supprimes_b2 = len(self.b2)
                        self.b2.clear()
                        for _ in range(worse_file.size - nombre_blocs_supprimes_b2):
                            oldest_key2 = next(iter(self.b1))  # Get the first key from the dictionary
                            self.b1.pop(oldest_key2)
                    if len(self.b1) >= self.adapte_B1:
                        for _ in range(self.adapte_B1):
                            oldest_key = next(iter(self.b1))  # Get the first key from the dictionary
                            self.b1.pop(oldest_key)
                    else:
                        nombre_blocs_supprimes_b1 = len(self.b1)
                        self.b1.clear()
                        for _ in range(worse_file.size - nombre_blocs_supprimes_b1):
                            oldest_key2 = next(iter(self.b2))  # Get the first key from the dictionary
                            self.b2.pop(oldest_key2)

    def remove_all(self, file: File):
        """
        Remove all blocks of a file from T1 and T2, and add them to B1 and B2 respectively.
        Reset the file's tier membership to 0 (not in SSD).

        :param file: The File object to remove from SSD (T1 or T2).
        """
        # Collect blocks from T1 and T2 that belong to this file
        blocks_t1 = [block for block in self.t1 if block[0] == file]
        blocks_t2 = [block for block in self.t2 if block[0] == file]

        # Remove those blocks from T1, place them in B1
        for block in blocks_t1:
            del self.t1[block]
            self.b1[block] = None

        # Remove those blocks from T2, place them in B2
        for block in blocks_t2:
            del self.t2[block]
            self.b2[block] = None

        # Remove file from file2blocks mapping
        if file in self.file2blocks:
            del self.file2blocks[file]

        # Indicate that the file is no longer in SSD
        self.file2tier[file] = 0

    def remove_all_hard(self, file: File):
        """
        Forcefully remove all blocks of a file from T1, T2, B1, and B2.
        Also removes the file from the SSD tier physically.

        :param file: The File object to remove completely.
        """
        if file in self.file2blocks:
            blocks = self.file2blocks[file]
            for block in blocks:
                self.t1.pop(block, None)
                self.t2.pop(block, None)
                self.b1.pop(block, None)
                self.b2.pop(block, None)
            del self.file2blocks[file]

        # Indicate that the file is no longer in SSD
        self.file2tier[file] = 0

        # Physically remove the file from the SSD tier
        self.ssd_tier.remove_file(file.name)

    def log_user_times(self):
        """
        Log each user's IOPS, throughput, and other usage stats in the log file,
        along with eviction miss percentage.
        """
        user_throughputs = self.calculate_user_throughput()

        # Calculate eviction miss percentage
        eviction_miss_percentage = (self.eviction_misses / self.misses) * 100 if self.misses > 0 else 0

        # Open the log file in append mode
        with open(self.log_file_path, 'a') as log_file:
            log_file.write(f"Time updated at: {datetime.datetime.now()}\n")
            log_file.write(f"Eviction Miss Percentage: {eviction_miss_percentage:.2f}%\n")

            # For each user, log usage and performance stats
            for user_id in self.users:
                time_spent = self.users[user_id].time_spent
                throughput = (
                    self.user_request_sizes[user_id] / self.user_total_time[user_id]
                ) if self.user_total_time[user_id] > 0 else 0
                iops = (
                    self.user_requests[user_id] / self.user_total_time[user_id]
                ) if self.user_total_time[user_id] > 0 else 0
                space_default = self.users[user_id].space_default
                space_used = self.users[user_id].space_utilization

                log_file.write(
                    f"User {user_id}: IOPS = {iops:.2f} ops/sec | "
                    f"Throughput = {throughput} bytes/sec | "
                    f"Total Time = {time_spent} s | "
                    f"Default Space = {space_default} | "
                    f"Used Space = {space_used}\n"
                )
            log_file.write("\n")

    def load_file_to(self, file, tier):
        """
        Load (move) a file to the specified tier.
        Ensures there is enough space in the cache by evicting files if necessary,
        and adds all of the file's blocks to the given tier structure.

        :param file: The File object to load into the tier.
        :param tier: The tier (T1 or T2) to which the file is loaded.
        """
        # Evict until there is enough space to load this file
        while file.size > (self.c - (len(self.t1) + len(self.t2))):
            self.evict()

        # Add each block of the file to the provided tier
        for block_offset in range(file.size):
            block = (file, block_offset)
            self.file2blocks[file].add(block)
            tier[block] = None

        # Mark that the file is now in SSD
        self.ssd_tier.add_file(file)
        self.file2tier[file] = 1

        # Increase the user's SSD space usage
        self.users[file.user_id].increase_space(file.size)

        # Track that this file is in the SSD set
        self.files_in_ssd.add(file)

    def move_file_to(self, file, tier):
        """
        Move a file from SSD to another tier or force reloading in the same tier structure
        (SSD -> remove_all_hard -> load_file_to).

        :param file: The File object to move.
        :param tier: The tier (T1 or T2) to which the file is moved.
        """
        # First remove all blocks from SSD completely
        self.remove_all_hard(file)

        # Then load file to the specified tier
        self.load_file_to(file, tier)

    def is_filename_in_b1(self, file_name):
        """
        Check if a file name is in the B1 ghost list.

        :param file_name: The name of the file to check.
        :return: True if the file name is in B1, False otherwise.
        """
        return any(file_obj.name == file_name for file_obj, _ in self.b1.keys())

    def is_filename_in_b2(self, file_name):
        """
        Check if a file name is in the B2 ghost list.

        :param file_name: The name of the file to check.
        :return: True if the file name is in B2, False otherwise.
        """
        return any(file_obj.name == file_name for file_obj, _ in self.b2.keys())

    def is_filename_in_t2(self, file_name):
        """
        Check if a file name is in T2 (actual SSD data in the second list).

        :param file_name: The name of the file to check.
        :return: True if the file name is in T2, False otherwise.
        """
        return any(file_obj.name == file_name for file_obj, _ in self.t2.keys())

    def is_filename_in_t1(self, file_name):
        """
        Check if a file name is in T1 (actual SSD data in the first list).

        :param file_name: The name of the file to check.
        :return: True if the file name is in T1, False otherwise.
        """
        return any(file_obj.name == file_name for file_obj, _ in self.t1.keys())

    def calculate_user_throughput(self):
        """
        Calculate and return a dictionary of user throughputs (bytes/sec).
        Throughput is defined here as (user_requests[user_id] / user_total_time[user_id])
        * user_request_sizes[user_id], if user_total_time > 0.

        :return: A dictionary mapping user_id to throughput value.
        """
        user_throughputs = {}
        for user_id in self.users:
            if self.user_total_time[user_id] > 0:
                throughput = (
                    (self.user_requests[user_id] / self.user_total_time[user_id]) *
                    self.user_request_sizes[user_id]
                )
                user_throughputs[user_id] = throughput
        return user_throughputs

    def _transfer_data_with_burst(self, data_size, read_tier=None, write_tier=None):
        """
        Utility function to transfer data_size bytes using a burst buffer
        between read_tier and write_tier.

        :param data_size: The total number of bytes to transfer.
        :param read_tier: The tier from which data is read.
        :param write_tier: The tier to which data is written.
        :return: (read_time, write_time) in seconds.
        """
        read_time = 0.0
        write_time = 0.0

        # Handle reading from read_tier
        if read_tier:
            remaining = data_size
            while remaining > 0:
                burst = max(remaining, self.burst_buffer_size)
                read_time += (burst / read_tier.read_throughput) + read_tier.latency
                remaining -= burst

        # Handle writing to write_tier
        if write_tier:
            remaining = data_size
            while remaining > 0:
                burst = max(remaining, self.burst_buffer_size)
                write_time += (burst / write_tier.write_throughput) + write_tier.latency
                remaining -= burst

        return read_time, write_time

    def evict_file(self, worse_file):
        """
        Evict the specified file from SSD to HDD and update relevant metrics.

        :param worse_file: The File object to evict.
        """
        # Decrease the user's SSD space usage
        self.users[worse_file.user_id].decrease_space(worse_file.size)

        # Remove the file from the SSD tier and add it to the HDD tier
        self.ssd_tier.remove_file(worse_file.name)
        self.hdd_tier.add_file(worse_file)

        # Update eviction counters
        self.evicted_blocks_count += worse_file.size
        self.evicted_file_count += 1

        # Remove all blocks of the file from cache structures
        self.remove_all(worse_file)

        # Add the evicted file to the list for burst transfer
        if not hasattr(self, 'evicted_files_during_request'):
            self.evicted_files_during_request = []
        self.evicted_files_during_request.append(worse_file)

        # Manage ghost lists B1 and B2 based on cache size constraints
        if (len(self.t1) + len(self.b1)) == self.c:
            if len(self.t1) < self.c:
                if len(self.b1) >= worse_file.size:
                    for _ in range(worse_file.size):
                        oldest_key2 = next(iter(self.b1))  # Get the first key from the dictionary
                        self.b1.pop(oldest_key2)
                else:
                    if len(self.b1) >= self.adapte_B1:
                        for _ in range(self.adapte_B1):
                            oldest_key = next(iter(self.b1))  # Get the first key from the dictionary
                            self.b1.pop(oldest_key)
                    else:
                        nombre_blocs_supprimes_b1 = len(self.b1)
                        self.b1.clear()
                        for _ in range(worse_file.size - nombre_blocs_supprimes_b1):
                            oldest_key2 = next(iter(self.b2))  # Get the first key from the dictionary
                            self.b2.pop(oldest_key2)
                    if len(self.b2) >= self.adapte_B2:
                        for _ in range(self.adapte_B2):
                            oldest_key2 = next(iter(self.b2))  # Get the first key from the dictionary
                            self.b2.pop(oldest_key2)
                    else:
                        nombre_blocs_supprimes_b2 = len(self.b2)
                        self.b2.clear()
                        for _ in range(worse_file.size - nombre_blocs_supprimes_b2):
                            oldest_key2 = next(iter(self.b1))  # Get the first key from the dictionary
                            self.b1.pop(oldest_key2)
        elif (len(self.t1) + len(self.t2) + len(self.b1) + len(self.b2)) >= self.c:
            if (len(self.t1) + len(self.t2) + len(self.b1) + len(self.b2)) == (self.c * 2):
                if len(self.b2) >= worse_file.size:
                    for _ in range(worse_file.size):
                        oldest_key2 = next(iter(self.b2))  # Get the first key from the dictionary
                        self.b2.pop(oldest_key2)
                else:
                    if len(self.b2) >= self.adapte_B2:
                        for _ in range(self.adapte_B2):
                            oldest_key2 = next(iter(self.b2))  # Get the first key from the dictionary
                            self.b2.pop(oldest_key2)
                    else:
                        nombre_blocs_supprimes_b2 = len(self.b2)
                        self.b2.clear()
                        for _ in range(worse_file.size - nombre_blocs_supprimes_b2):
                            oldest_key2 = next(iter(self.b1))  # Get the first key from the dictionary
                            self.b1.pop(oldest_key2)
                    if len(self.b1) >= self.adapte_B1:
                        for _ in range(self.adapte_B1):
                            oldest_key = next(iter(self.b1))  # Get the first key from the dictionary
                            self.b1.pop(oldest_key)
                    else:
                        nombre_blocs_supprimes_b1 = len(self.b1)
                        self.b1.clear()
                        for _ in range(worse_file.size - nombre_blocs_supprimes_b1):
                            oldest_key2 = next(iter(self.b2))  # Get the first key from the dictionary
                            self.b2.pop(oldest_key2)

    def on_io(self, file, timestamp, requestType, offsetStart, offsetEnd):
        """
        Handle an IO operation on the given file from offsetStart to offsetEnd at the specified timestamp.
        This function processes hits/misses, evictions, and possible block movements between HDD and SSD.

        :param file: The File object being accessed.
        :param timestamp: The timestamp of the IO operation.
        :param requestType: The type of request (read/write).
        :param offsetStart: The starting block offset.
        :param offsetEnd: The ending block offset (non-inclusive).
        """
        # Reset the list of evictions for this request
        self.evicted_files_during_request = []

        # Reset time counters for this IO
        self.total_time = 0
        self.ssd_time = 0
        self.hdd_time = 0

        # Update current time
        self.time_now = timestamp
        self.timestamp = timestamp

        # Assume this is not a new file unless proven otherwise
        self.new_file = False

        # Increment the number of IO operations processed so far
        self.io_counter += 1

        # Identify user
        user_id = file.user_id

        # Calculate request size in blocks and in bytes
        request_size_in_blocks = (offsetEnd - offsetStart)
        request_size_in_bytes = request_size_in_blocks * self.block_size

        # Update user metrics
        self.user_request_sizes[user_id] += request_size_in_bytes
        self.user_requests[user_id] += 1

        # Track whether we have recorded if this miss is an eviction miss or cold miss
        miss_type_recorded = False

        # If the file was previously evicted from SSD, log its reloading
        if file in self.evicted_files_SSD:
            with open(self.eviction_log_file, 'a') as log_file:
                log_file.write(f"Reloading evicted file into SSD: {file.name}\n")
                log_file.write(f"Timestamp: {timestamp}\n")
                log_file.write('--------------------------------------------\n')

        # Iterate over each block of the request
        for block_offset in range(offsetStart, offsetEnd):
            block = (file, block_offset)

            # Check if block is a hit (in T1 or T2) or a miss
            if (block in self.t1 and not self.new_file) or (block in self.t2):
                self.hits += 1
            else:
                self.misses += 1
                if not miss_type_recorded:
                    if file in self.evicted_files:
                        self.eviction_misses += 1
                    elif file not in self.files_in_ssd:
                        self.cold_misses += 1
                    miss_type_recorded = True

            # Handle block accesses based on their current location
            if block in self.t1:
                # Immediate SSD read
                if not self.new_file:
                    self.ssd_time += (self.block_size / self.ssd_tier.read_throughput) + self.ssd_tier.latency
                    del self.t1[block]
                    self.t2[block] = None
                else:
                    self.ssd_time += (self.block_size / self.ssd_tier.read_throughput) + self.ssd_tier.latency

            elif block in self.t2:
                # Immediate SSD read
                self.ssd_time += (self.block_size / self.ssd_tier.read_throughput) + self.ssd_tier.latency
                del self.t2[block]
                self.t2[block] = None

            elif block in self.b1:
                # Adjust the adaptation parameter p
                self.p = min(self.p + max(round(len(self.b2) / (len(self.b1))), file.size), self.c)
                self.move_file_to(file, self.t2)
                self.hdd_tier.remove_file(file.name)

                # Immediate HDD->SSD transfer for the entire file
                data_size_file = file.size * self.block_size
                hdd_read, ssd_write = self._transfer_data_with_burst(data_size_file, self.hdd_tier, self.ssd_tier)
                self.hdd_time += hdd_read
                self.ssd_time += ssd_write

            elif block in self.b2:
                # Adjust the adaptation parameter p
                self.p = max(self.p - max(round(len(self.b1) / (len(self.b2))), file.size), 0)
                self.move_file_to(file, self.t2)
                self.hdd_tier.remove_file(file.name)

                # Immediate HDD->SSD transfer for the entire file
                data_size_file = file.size * self.block_size
                hdd_read, ssd_write = self._transfer_data_with_burst(data_size_file, self.hdd_tier, self.ssd_tier)
                self.hdd_time += hdd_read
                self.ssd_time += ssd_write

            elif self.hdd_tier.is_file_in_tier(file.name) and not self.is_filename_in_b1(file.name) and not self.is_filename_in_b2(file.name):
                # If the file is on HDD but not in B1 or B2
                if file.size <= self.c:
                    # If the file can fit in SSD
                    self.new_file = True
                    self.hdd_tier.remove_file(file.name)
                    self.remove_all_hard(file)
                    self.load_file_to(file, self.t1)
                    self.file2tier[file] = 1

                    # Immediate HDD->SSD transfer
                    data_size_file = file.size * self.block_size
                    hdd_read, ssd_write = self._transfer_data_with_burst(data_size_file, self.hdd_tier, self.ssd_tier)
                    self.hdd_time += hdd_read
                    self.ssd_time += ssd_write
                else:
                    # Immediate HDD read block by block
                    hdd_read = (self.block_size / self.hdd_tier.read_throughput)  # + self.hdd_tier.latency
                    self.hdd_time += hdd_read
                    self.nbr_of_blocks_hdd_reads += 1

            else:
                # If the file is not on HDD or SSD, it must be a new file
                if not self.hdd_tier.is_file_in_tier(file.name) and not self.ssd_tier.is_file_in_tier(file.name):
                    if file.size <= self.c:
                        # If file can fit in SSD
                        self.new_file = True
                        self.load_file_to(file, self.t1)
                        self.file2tier[file] = 1

                        # Immediate SSD write for the block
                        data_size_file = self.block_size
                        ssd_write = (data_size_file / self.ssd_tier.write_throughput)  # + self.ssd_tier.latency
                        self.ssd_time += ssd_write
                    else:
                        # Immediate HDD read block by block
                        self.hdd_tier.add_file(file)
                        hdd_read = (self.block_size / self.hdd_tier.read_throughput) + self.hdd_tier.latency
                        self.hdd_time += hdd_read
                        self.nbr_of_blocks_hdd_reads += 1

        # After processing all blocks, handle the SSD->HDD evictions
        # that happened during this IO in one burst transfer
        if hasattr(self, 'evicted_files_during_request') and self.evicted_files_during_request:
            total_data_size = sum(f.size for f in self.evicted_files_during_request) * self.block_size
            ssd_read_time, hdd_write_time = self._transfer_data_with_burst(total_data_size, self.ssd_tier, self.hdd_tier)
            self.ssd_time += ssd_read_time
            self.hdd_time += hdd_write_time

        # Calculate total time spent on this IO operation
        self.total_time += self.ssd_time + self.hdd_time

        # Update user total time
        self.user_total_time[file.user_id] += self.total_time

        # Update time spent for the user object
        self.users[file.user_id].increase_time_spent(self.total_time)

        # Debug print for verifying the IO counter
        print(self.io_counter)

        # If this IO is the last one, log user times
        if self.io_counter == self.total_io_operations:
            print("f It's the last file")
            self.log_user_times()
            self.io_counter = 0

    def calculate_criteria_scores(self):
        """
        Calculate the scores for each file based on position, lifetime, and fairness.

        :return: A dictionary mapping each file to its criteria scores as a numpy array.
        """
        file2position_score = defaultdict(float)
        file2lifetime_score = defaultdict(float)
        file2fairness_score = defaultdict(float)

        # Calculate position-based scores
        for i, (file, _) in enumerate(self.t1):
            file2position_score[file] += len(self.t1) - i

        for j, (file, _) in enumerate(self.t2):
            file2position_score[file] += len(self.t2) - j

        # Normalize position scores by file size
        for file in file2position_score.keys():
            file2position_score[file] /= file.size

        # Calculate lifetime and fairness scores
        for file in self.file2blocks:
            time_diff = (file.lifetime - (self.time_now - file.firstAccessTime)) / 1000
            file2lifetime_score[file] = np.exp(- np.float64(time_diff))
            if self.users[file.user_id].space_default > 0:
                file2fairness_score[file] = (self.users[file.user_id].space_utilization /
                                             self.users[file.user_id].space_default)

        # Create a dictionary of combined scores
        scores = {file: np.array([file2position_score[file],
                                  file2lifetime_score[file],
                                  file2fairness_score[file]]) for file in self.file2blocks}
        return scores

    def normalize_scores(self, scores):
        """
        Normalize the criteria scores using vector normalization.

        :param scores: A dictionary mapping files to their criteria scores.
        :return: The normalized scores as a dictionary.
        """
        norms = np.linalg.norm(list(scores.values()), axis=0)
        for file in scores:
            scores[file] /= norms
        return scores

    def calculate_topsis_scores(self, scores):
        """
        Calculate the TOPSIS scores for each file based on normalized criteria scores.

        :param scores: A dictionary mapping files to their normalized criteria scores.
        :return: A dictionary mapping files to their TOPSIS scores.
        """
        weights = np.array([1 / 3, 1 / 3, 1 / 3])  # Equal weights for all criteria
        weighted_scores = {file: score * weights for file, score in scores.items()}
        ideal_best = np.max(list(weighted_scores.values()), axis=0)
        ideal_worst = np.min(list(weighted_scores.values()), axis=0)

        topsis_scores = {}
        for file, score in weighted_scores.items():
            distance_to_best = np.linalg.norm(score - ideal_best)
            distance_to_worst = np.linalg.norm(score - ideal_worst)
            topsis_scores[file] = np.float64(distance_to_worst) / (distance_to_best + distance_to_worst)
        return topsis_scores

    def evict_file(self, worse_file):
        """
        Evict the specified file from SSD to HDD and update relevant metrics.

        :param worse_file: The File object to evict.
        """
        # Decrease the user's SSD space usage
        self.users[worse_file.user_id].decrease_space(worse_file.size)

        # Remove the file from the SSD tier and add it to the HDD tier
        self.ssd_tier.remove_file(worse_file.name)
        self.hdd_tier.add_file(worse_file)

        # Update eviction counters
        self.evicted_blocks_count += worse_file.size
        self.evicted_file_count += 1

        # Remove all blocks of the file from cache structures
        self.remove_all(worse_file)

        # Add the evicted file to the list for burst transfer
        if not hasattr(self, 'evicted_files_during_request'):
            self.evicted_files_during_request = []
        self.evicted_files_during_request.append(worse_file)

        # Manage ghost lists B1 and B2 based on cache size constraints
        if (len(self.t1) + len(self.b1)) == self.c:
            if len(self.t1) < self.c:
                if len(self.b1) >= worse_file.size:
                    for _ in range(worse_file.size):
                        oldest_key2 = next(iter(self.b1))  # Get the first key from the dictionary
                        self.b1.pop(oldest_key2)
                else:
                    if len(self.b1) >= self.adapte_B1:
                        for _ in range(self.adapte_B1):
                            oldest_key = next(iter(self.b1))  # Get the first key from the dictionary
                            self.b1.pop(oldest_key)
                    else:
                        nombre_blocs_supprimes_b1 = len(self.b1)
                        self.b1.clear()
                        for _ in range(worse_file.size - nombre_blocs_supprimes_b1):
                            oldest_key2 = next(iter(self.b2))  # Get the first key from the dictionary
                            self.b2.pop(oldest_key2)
                    if len(self.b2) >= self.adapte_B2:
                        for _ in range(self.adapte_B2):
                            oldest_key2 = next(iter(self.b2))  # Get the first key from the dictionary
                            self.b2.pop(oldest_key2)
                    else:
                        nombre_blocs_supprimes_b2 = len(self.b2)
                        self.b2.clear()
                        for _ in range(worse_file.size - nombre_blocs_supprimes_b2):
                            oldest_key2 = next(iter(self.b1))  # Get the first key from the dictionary
                            self.b1.pop(oldest_key2)
        elif (len(self.t1) + len(self.t2) + len(self.b1) + len(self.b2)) >= self.c:
            if (len(self.t1) + len(self.t2) + len(self.b1) + len(self.b2)) == (self.c * 2):
                if len(self.b2) >= worse_file.size:
                    for _ in range(worse_file.size):
                        oldest_key2 = next(iter(self.b2))  # Get the first key from the dictionary
                        self.b2.pop(oldest_key2)
                else:
                    if len(self.b2) >= self.adapte_B2:
                        for _ in range(self.adapte_B2):
                            oldest_key2 = next(iter(self.b2))  # Get the first key from the dictionary
                            self.b2.pop(oldest_key2)
                    else:
                        nombre_blocs_supprimes_b2 = len(self.b2)
                        self.b2.clear()
                        for _ in range(worse_file.size - nombre_blocs_supprimes_b2):
                            oldest_key2 = next(iter(self.b1))  # Get the first key from the dictionary
                            self.b1.pop(oldest_key2)
                    if len(self.b1) >= self.adapte_B1:
                        for _ in range(self.adapte_B1):
                            oldest_key = next(iter(self.b1))  # Get the first key from the dictionary
                            self.b1.pop(oldest_key)
                    else:
                        nombre_blocs_supprimes_b1 = len(self.b1)
                        self.b1.clear()
                        for _ in range(worse_file.size - nombre_blocs_supprimes_b1):
                            oldest_key2 = next(iter(self.b2))  # Get the first key from the dictionary
                            self.b2.pop(oldest_key2)

    def log_user_times(self):
        """
        Log each user's IOPS, throughput, and other usage stats in the log file,
        along with eviction miss percentage.
        """
        user_throughputs = self.calculate_user_throughput()

        # Calculate eviction miss percentage
        eviction_miss_percentage = (self.eviction_misses / self.misses) * 100 if self.misses > 0 else 0

        # Open the log file in append mode
        with open(self.log_file_path, 'a') as log_file:
            log_file.write(f"Time updated at: {datetime.datetime.now()}\n")
            log_file.write(f"Eviction Miss Percentage: {eviction_miss_percentage:.2f}%\n")

            # For each user, log usage and performance stats
            for user_id in self.users:
                time_spent = self.users[user_id].time_spent
                throughput = (
                    self.user_request_sizes[user_id] / self.user_total_time[user_id]
                ) if self.user_total_time[user_id] > 0 else 0
                iops = (
                    self.user_requests[user_id] / self.user_total_time[user_id]
                ) if self.user_total_time[user_id] > 0 else 0
                space_default = self.users[user_id].space_default
                space_used = self.users[user_id].space_utilization

                log_file.write(
                    f"User {user_id}: IOPS = {iops:.2f} ops/sec | "
                    f"Throughput = {throughput} bytes/sec | "
                    f"Total Time = {time_spent} s | "
                    f"Default Space = {space_default} | "
                    f"Used Space = {space_used}\n"
                )
            log_file.write("\n")

    def load_file_to(self, file, tier):
        """
        Load (move) a file to the specified tier (usually SSD).
        Ensures there is enough space in the cache by evicting files if necessary,
        and adds all of the file's blocks to the given tier structure.

        :param file: The File object to load into the tier.
        :param tier: The tier (T1 or T2) to which the file is loaded.
        """
        # Evict until there is enough space to load this file
        while file.size > (self.c - (len(self.t1) + len(self.t2))):
            self.evict()

        # Add each block of the file to the provided tier
        for block_offset in range(file.size):
            block = (file, block_offset)
            self.file2blocks[file].add(block)
            tier[block] = None

        # Mark that the file is now in SSD
        self.ssd_tier.add_file(file)
        self.file2tier[file] = 1

        # Increase the user's SSD space usage
        self.users[file.user_id].increase_space(file.size)

        # Track that this file is in the SSD set
        self.files_in_ssd.add(file)

    def move_file_to(self, file, tier):
        """
        Move a file from SSD to another tier or force reloading in the same tier structure
        (SSD -> remove_all_hard -> load_file_to).

        :param file: The File object to move.
        :param tier: The tier (T1 or T2) to which the file is moved.
        """
        # First remove all blocks from SSD completely
        self.remove_all_hard(file)

        # Then load file to the specified tier
        self.load_file_to(file, tier)

    def is_filename_in_b1(self, file_name):
        """
        Check if a file name is in the B1 ghost list.

        :param file_name: The name of the file to check.
        :return: True if the file name is in B1, False otherwise.
        """
        return any(file_obj.name == file_name for file_obj, _ in self.b1.keys())

    def is_filename_in_b2(self, file_name):
        """
        Check if a file name is in the B2 ghost list.

        :param file_name: The name of the file to check.
        :return: True if the file name is in B2, False otherwise.
        """
        return any(file_obj.name == file_name for file_obj, _ in self.b2.keys())

    def is_filename_in_t2(self, file_name):
        """
        Check if a file name is in T2 (actual SSD data in the second list).

        :param file_name: The name of the file to check.
        :return: True if the file name is in T2, False otherwise.
        """
        return any(file_obj.name == file_name for file_obj, _ in self.t2.keys())

    def is_filename_in_t1(self, file_name):
        """
        Check if a file name is in T1 (actual SSD data in the first list).

        :param file_name: The name of the file to check.
        :return: True if the file name is in T1, False otherwise.
        """
        return any(file_obj.name == file_name for file_obj, _ in self.t1.keys())

    def calculate_user_throughput(self):
        """
        Calculate and return a dictionary of user throughputs (bytes/sec).
        Throughput is defined here as (user_requests[user_id] / user_total_time[user_id])
        * user_request_sizes[user_id], if user_total_time > 0.

        :return: A dictionary mapping user_id to throughput value.
        """
        user_throughputs = {}
        for user_id in self.users:
            if self.user_total_time[user_id] > 0:
                throughput = (
                    (self.user_requests[user_id] / self.user_total_time[user_id]) *
                    self.user_request_sizes[user_id]
                )
                user_throughputs[user_id] = throughput
        return user_throughputs

    def _transfer_data_with_burst(self, data_size, read_tier=None, write_tier=None):
        """
        Utility function to transfer data_size bytes using a burst buffer
        between read_tier and write_tier.

        :param data_size: The total number of bytes to transfer.
        :param read_tier: The tier from which data is read.
        :param write_tier: The tier to which data is written.
        :return: (read_time, write_time) in seconds.
        """
        read_time = 0.0
        write_time = 0.0

        # Handle reading from read_tier
        if read_tier:
            remaining = data_size
            while remaining > 0:
                burst = max(remaining, self.burst_buffer_size)
                read_time += (burst / read_tier.read_throughput) + read_tier.latency
                remaining -= burst

        # Handle writing to write_tier
        if write_tier:
            remaining = data_size
            while remaining > 0:
                burst = max(remaining, self.burst_buffer_size)
                write_time += (burst / write_tier.write_throughput) + write_tier.latency
                remaining -= burst

        return read_time, write_time

    def on_io(self, file, timestamp, requestType, offsetStart, offsetEnd):
        """
        Handle an IO operation on the given file from offsetStart to offsetEnd at the specified timestamp.
        This function processes hits/misses, evictions, and possible file movements between HDD and SSD.

        :param file: The File object being accessed.
        :param timestamp: The timestamp of the IO operation.
        :param requestType: The type of request (read/write).
        :param offsetStart: The starting block offset.
        :param offsetEnd: The ending block offset (non-inclusive).
        """
        # Reset the list of evictions for this request
        self.evicted_files_during_request = []

        # Reset time counters for this IO
        self.total_time = 0
        self.ssd_time = 0
        self.hdd_time = 0

        # Update current time
        self.time_now = timestamp
        self.timestamp = timestamp

        # Assume this is not a new file unless proven otherwise
        self.new_file = False

        # Increment the number of IO operations processed so far
        self.io_counter += 1

        # Identify user
        user_id = file.user_id

        # Calculate request size in blocks and in bytes
        request_size_in_blocks = (offsetEnd - offsetStart)
        request_size_in_bytes = request_size_in_blocks * self.block_size

        # Update user metrics
        self.user_request_sizes[user_id] += request_size_in_bytes
        self.user_requests[user_id] += 1

        # Track whether we have recorded if this miss is an eviction miss or cold miss
        miss_type_recorded = False

        # If the file was previously evicted from SSD, log its reloading
        if file in self.evicted_files_SSD:
            with open(self.eviction_log_file, 'a') as log_file:
                log_file.write(f"Reloading evicted file into SSD: {file.name}\n")
                log_file.write(f"Timestamp: {timestamp}\n")
                log_file.write('--------------------------------------------\n')

        # Iterate over each block of the request
        for block_offset in range(offsetStart, offsetEnd):
            block = (file, block_offset)

            # Check if block is a hit (in T1 or T2) or a miss
            if (block in self.t1 and not self.new_file) or (block in self.t2):
                self.hits += 1
            else:
                self.misses += 1
                if not miss_type_recorded:
                    if file in self.evicted_files:
                        self.eviction_misses += 1
                    elif file not in self.files_in_ssd:
                        self.cold_misses += 1
                    miss_type_recorded = True

            # Handle block accesses based on their current location
            if block in self.t1:
                # Immediate SSD read
                if not self.new_file:
                    self.ssd_time += (self.block_size / self.ssd_tier.read_throughput) + self.ssd_tier.latency
                    del self.t1[block]
                    self.t2[block] = None
                else:
                    self.ssd_time += (self.block_size / self.ssd_tier.read_throughput) + self.ssd_tier.latency

            elif block in self.t2:
                # Immediate SSD read
                self.ssd_time += (self.block_size / self.ssd_tier.read_throughput) + self.ssd_tier.latency
                del self.t2[block]
                self.t2[block] = None

            elif block in self.b1:
                # Adjust the adaptation parameter p
                self.p = min(self.p + max(round(len(self.b2) / (len(self.b1))), file.size), self.c)
                self.move_file_to(file, self.t2)
                self.hdd_tier.remove_file(file.name)

                # Immediate HDD->SSD transfer for the entire file
                data_size_file = file.size * self.block_size
                hdd_read, ssd_write = self._transfer_data_with_burst(data_size_file, self.hdd_tier, self.ssd_tier)
                self.hdd_time += hdd_read
                self.ssd_time += ssd_write

            elif block in self.b2:
                # Adjust the adaptation parameter p
                self.p = max(self.p - max(round(len(self.b1) / (len(self.b2))), file.size), 0)
                self.move_file_to(file, self.t2)
                self.hdd_tier.remove_file(file.name)

                # Immediate HDD->SSD transfer for the entire file
                data_size_file = file.size * self.block_size
                hdd_read, ssd_write = self._transfer_data_with_burst(data_size_file, self.hdd_tier, self.ssd_tier)
                self.hdd_time += hdd_read
                self.ssd_time += ssd_write

            elif self.hdd_tier.is_file_in_tier(file.name) and not self.is_filename_in_b1(file.name) and not self.is_filename_in_b2(file.name):
                # If the file is on HDD but not in B1 or B2
                if file.size <= self.c:
                    # If the file can fit in SSD
                    self.new_file = True
                    self.hdd_tier.remove_file(file.name)
                    self.remove_all_hard(file)
                    self.load_file_to(file, self.t1)
                    self.file2tier[file] = 1

                    # Immediate HDD->SSD transfer
                    data_size_file = file.size * self.block_size
                    hdd_read, ssd_write = self._transfer_data_with_burst(data_size_file, self.hdd_tier, self.ssd_tier)
                    self.hdd_time += hdd_read
                    self.ssd_time += ssd_write
                else:
                    # Immediate HDD read block by block
                    hdd_read = (self.block_size / self.hdd_tier.read_throughput)  # + self.hdd_tier.latency
                    self.hdd_time += hdd_read
                    self.nbr_of_blocks_hdd_reads += 1

            else:
                # If the file is not on HDD or SSD, it must be a new file
                if not self.hdd_tier.is_file_in_tier(file.name) and not self.ssd_tier.is_file_in_tier(file.name):
                    if file.size <= self.c:
                        # If file can fit in SSD
                        self.new_file = True
                        self.load_file_to(file, self.t1)
                        self.file2tier[file] = 1

                        # Immediate SSD write for the block
                        data_size_file = self.block_size
                        ssd_write = (data_size_file / self.ssd_tier.write_throughput)  # + self.ssd_tier.latency
                        self.ssd_time += ssd_write
                    else:
                        # Immediate HDD read block by block
                        self.hdd_tier.add_file(file)
                        hdd_read = (self.block_size / self.hdd_tier.read_throughput) + self.hdd_tier.latency
                        self.hdd_time += hdd_read
                        self.nbr_of_blocks_hdd_reads += 1

        # After processing all blocks, handle the SSD->HDD evictions
        # that happened during this IO in one burst transfer
        if hasattr(self, 'evicted_files_during_request') and self.evicted_files_during_request:
            total_data_size = sum(f.size for f in self.evicted_files_during_request) * self.block_size
            ssd_read_time, hdd_write_time = self._transfer_data_with_burst(total_data_size, self.ssd_tier, self.hdd_tier)
            self.ssd_time += ssd_read_time
            self.hdd_time += hdd_write_time

        # Calculate total time spent on this IO operation
        self.total_time += self.ssd_time + self.hdd_time

        # Update user total time
        self.user_total_time[file.user_id] += self.total_time

        # Update time spent for the user object
        self.users[file.user_id].increase_time_spent(self.total_time)

        # Debug print for verifying the IO counter
        print(self.io_counter)

        # If this IO is the last one, log user times
        if self.io_counter == self.total_io_operations:
            print("f It's the last file")
            self.log_user_times()
            self.io_counter = 0
