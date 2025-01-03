# -*- coding: utf-8 -*-
from collections import defaultdict
from structures.file import File
from policy.policy import Policy
import datetime
import heapq
import itertools

class LFU(Policy):
    """
    LFU is a caching policy that evicts the least frequently used block when the cache is full.
    """

    def __init__(self, cache_size, users, alpha, ssd_tier, hdd_tier, log_file_path, total_io_count):
        """
        Initialize the LFU_bb_l caching policy instance.

        :param cache_size: The maximum capacity (in number of blocks) of the SSD cache.
        :param users: A dictionary (or similar mapping) of users.
        :param alpha: A parameter (unused directly here, kept for policy interface consistency).
        :param ssd_tier: The SSD tier that manages read/write throughput and latency.
        :param hdd_tier: The HDD tier that manages read/write throughput and latency.
        :param log_file_path: The file path for logging user times and throughput.
        :param total_io_count: The total number of IO operations for this simulation.
        """
        super().__init__(cache_size, alpha, ssd_tier, hdd_tier)

        self.users = users
        self.total_io_operations = total_io_count
        self.log_file_path = log_file_path

        # Mapping (file_id, block_offset) -> block_id
        self.block_id_map = {}
        self.block_id_counter = 0

        # Metadata about each block: block_id -> (file_name, user_id, offset)
        self.block_metadata = {}

        # Unified structure for LFU:
        # lfu_data[block_id] = {'freq': frequency, 'count': insertion_count}
        self.lfu_data = {}
        # A min-heap storing entries as tuples (frequency, insertion_count, block_id)
        self.lfu_heap = []

        # Statistics
        self.hits = 0
        self.misses = 0
        self.ssd_time = 0
        self.hdd_time = 0
        self.evicted_blocks_count = 0

        # User-related metrics
        self.user_requests = defaultdict(int)
        self.user_request_sizes = defaultdict(int)
        self.user_total_time = defaultdict(float)
        self.io_counter = 0

        # Cache configuration
        self.block_size = 4 * 1024         # 4 KB block size
        self.burst_buffer_size = 256 * 1024  # 256 KB burst buffer size
        self.c = cache_size                # cache capacity in blocks

        # Tiers
        self.ssd_tier = ssd_tier
        self.hdd_tier = hdd_tier

        self.evicted_file_count = 0
        self.migration_times = 0
        self.prefetch_times = 0
        self.read_times = 0
        self.write_times = 0
        self.migration_times_evict = 0
        self.nbr_of_blocks_hdd_reads = 0

        # Counter to timestamp heap entries, ensuring stable sorting
        self.counter = itertools.count()

    def _transfer_data_with_burst(self, data_size, read_tier=None, write_tier=None):
        """
        Transfer data by 'full burst', as done in Arc_block_Cache.
        This simulates data movement from one tier to another using a burst buffer.

        :param data_size: The total size (in bytes) to transfer.
        :param read_tier: The tier from which data is read (if any).
        :param write_tier: The tier to which data is written (if any).
        :return: (read_time, write_time) in seconds.
        """
        read_time = 0.0
        write_time = 0.0

        # Handle reading from read_tier
        if read_tier:
            remaining = data_size
            while remaining > 0:
                burst = self.burst_buffer_size
                read_time += (burst / read_tier.read_throughput) + read_tier.latency
                remaining -= self.burst_buffer_size

        # Handle writing to write_tier
        if write_tier:
            remaining = data_size
            while remaining > 0:
                burst = self.burst_buffer_size
                write_time += (burst / write_tier.write_throughput) + write_tier.latency
                remaining -= self.burst_buffer_size

        return read_time, write_time

    def get_block_id(self, file_obj, block_offset):
        """
        Return a unique block_id for (file_name, block_offset).
        If it doesn't exist yet, create it.

        :param file_obj: The File object for which we need a block_id.
        :param block_offset: The offset within the file (in blocks).
        :return: The unique block_id corresponding to (file_name, block_offset).
        """
        name = file_obj.name
        key = (name, block_offset)
        if key not in self.block_id_map:
            self.block_id_map[key] = self.block_id_counter
            self.block_id_counter += 1
            # Store block metadata
            self.block_metadata[self.block_id_map[key]] = (name, file_obj.user_id, block_offset)
        return self.block_id_map[key]

    def evict(self):
        """
        Evict blocks using an LFU strategy.
        Does not directly compute transfer time, just returns the evicted blocks.

        :return: A list of block_ids that were evicted.
        """
        evicted_blocks = []
        while len(self.lfu_data) >= self.c:
            if not self.lfu_heap:
                break
            freq, count, block_id = heapq.heappop(self.lfu_heap)

            # Check coherence: see if block_id is still in lfu_data with the same freq
            if block_id in self.lfu_data and self.lfu_data[block_id]['freq'] == freq:
                # Retrieve user information
                name, user_id, offset = self.block_metadata[block_id]

                # Update user's SSD usage
                self.users[user_id].decrease_space(1)
                self.evicted_blocks_count += 1

                # Remove the block from the SSD
                self.ssd_tier.remove_block(name, offset)
                # Add it to the HDD
                self.hdd_tier.add_block(name, offset)

                # Remove from lfu_data
                del self.lfu_data[block_id]

                # Track this block_id as evicted for grouped transfer
                evicted_blocks.append(block_id)

        return evicted_blocks

    def on_io(self, file, timestamp, requestType, offsetStart, offsetEnd):
        """
        Handle an IO request for the given file from offsetStart to offsetEnd at a specific timestamp.

        :param file: The File object being accessed.
        :param timestamp: The time at which the IO occurred.
        :param requestType: Type of request (read or write).
        :param offsetStart: The first block offset of the request.
        :param offsetEnd: The last block offset of the request (non-inclusive).
        """
        self.ssd_time = 0
        self.hdd_time = 0
        self.total_time = 0
        self.io_counter += 1

        user_id = file.user_id
        request_size_in_blocks = (offsetEnd - offsetStart)
        request_size_in_bytes = request_size_in_blocks * self.block_size

        # Update user metrics
        self.user_requests[user_id] += 1
        self.user_request_sizes[user_id] += request_size_in_bytes

        evicted_blocks_during_request = []

        # Process each block in the request
        for block_offset in range(offsetStart, offsetEnd):
            block_id = self.get_block_id(file, block_offset)

            # Check if the block is already in the cache
            if block_id in self.lfu_data:
                self.hits += 1
                # Increase the frequency
                self.lfu_data[block_id]['freq'] += 1
                new_freq = self.lfu_data[block_id]['freq']
                # Insert a new entry in the heap for updated frequency
                count = next(self.counter)
                heapq.heappush(self.lfu_heap, (new_freq, count, block_id))

                # Reading 1 block from SSD
                self.ssd_time += (self.block_size / self.ssd_tier.read_throughput) + self.ssd_tier.latency

            else:
                self.misses += 1
                # If the cache is full, evict blocks
                if len(self.lfu_data) >= self.c:
                    evicted_blocks = self.evict()
                    evicted_blocks_during_request.extend(evicted_blocks)

                name, user_id, _ = self.block_metadata[block_id]

                # Check if block is on HDD
                if self.hdd_tier.is_block_in_file(name, block_offset):
                    # Move block from HDD to SSD
                    self.hdd_tier.remove_block(name, block_offset)
                    self.ssd_tier.add_block(name, block_offset)
                    self.users[user_id].increase_space(1)

                    # Initialize LFU data
                    self.lfu_data[block_id] = {'freq': 1, 'count': next(self.counter)}
                    heapq.heappush(self.lfu_heap, (1, self.lfu_data[block_id]['count'], block_id))

                    # Transfer times for 1 block (HDD->SSD)
                    self.ssd_time += (self.block_size / self.ssd_tier.write_throughput) + self.ssd_tier.latency
                    self.hdd_time += (self.block_size / self.hdd_tier.read_throughput) + self.hdd_tier.latency

                else:
                    # If the block doesn't exist anywhere, create it in SSD
                    self.ssd_tier.add_block(name, block_offset)
                    self.users[user_id].increase_space(1)

                    # Initialize LFU data
                    self.lfu_data[block_id] = {'freq': 1, 'count': next(self.counter)}
                    heapq.heappush(self.lfu_heap, (1, self.lfu_data[block_id]['count'], block_id))

                    # Write time for 1 block to SSD
                    self.ssd_time += (self.block_size / self.ssd_tier.write_throughput) + self.ssd_tier.latency

        # After processing all blocks, if we evicted blocks to HDD, do a grouped transfer
        if evicted_blocks_during_request:
            total_data_size = len(evicted_blocks_during_request) * self.block_size
            ssd_read_time, hdd_write_time = self._transfer_data_with_burst(
                total_data_size, self.ssd_tier, self.hdd_tier
            )
            self.ssd_time += ssd_read_time
            self.hdd_time += hdd_write_time

        self.total_time += self.ssd_time + self.hdd_time

        # Update user time spent
        self.users[user_id].increase_time_spent(self.total_time)
        self.user_total_time[user_id] += self.total_time

        # If we have reached the total number of IO operations, log user stats
        if self.io_counter == self.total_io_operations:
            self.log_user_times()
            self.io_counter = 0

    def log_user_times(self):
        """
        Log user throughput, total time, and space usage in the specified log file.
        """
        with open(self.log_file_path, 'a') as log_file:
            log_file.write(f"Time updated at: {datetime.datetime.now()}\n")
            for user_id in self.users:
                time_spent = self.users[user_id].time_spent
                space_default = self.users[user_id].space_default
                space_used = self.users[user_id].space_utilization
                throughput = (self.user_request_sizes[user_id] / self.user_total_time[user_id]
                              if self.user_total_time[user_id] > 0 else 0)
                log_file.write(
                    f"User {user_id}: Throughput = {throughput:.2f} bytes/sec | "
                    f"Total Time = {time_spent:.2f} seconds | "
                    f"Default Space = {space_default} | "
                    f"Used Space = {space_used}\n"
                )
            log_file.write("\n")
