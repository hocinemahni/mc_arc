# -*- coding: utf-8 -*-
from collections import defaultdict, OrderedDict
from structures.file import File
from policy.policy import Policy
import datetime

class LRU_block_bb(Policy):
    """
    LRU_block_bb is a caching policy that manages blocks between SSD and HDD tiers
    using the Least Recently Used (LRU) algorithm. It incorporates a burst buffer
    mechanism for efficient data transfers and handles block evictions from SSD to HDD.
    """

    def __init__(self, cache_size, users, alpha, ssd_tier, hdd_tier, log_file_path, total_io_count):
        """
        Initialize the LRU_block_bb caching policy instance.

        :param cache_size: The maximum capacity (in number of blocks) of the SSD cache.
        :param users: A dictionary (or similar mapping) of users.
        :param alpha: A parameter used by the policy for adaptation.
        :param ssd_tier: The SSD tier that manages read/write throughput and latency.
        :param hdd_tier: The HDD tier that manages read/write throughput and latency.
        :param log_file_path: The file path for logging user times and throughput.
        :param total_io_count: The total number of IO operations for this simulation.
        """
        super().__init__(cache_size, alpha, ssd_tier, hdd_tier)
        
        self.users = users
        self.log_file_path = log_file_path
        self.total_io_operations = total_io_count
        self.io_counter = 0

        # Initialize LRU cache using OrderedDict
        self.lru_list = OrderedDict()
        self.c = cache_size
        self.alpha = alpha

        # Block size = 4KB
        self.block_size = 4 * 1024

        # Burst buffer size = 256KB
        self.burst_buffer_size = 256 * 1024

        # Tier management
        self.ssd_tier = ssd_tier
        self.hdd_tier = hdd_tier

        # Metrics
        self.hits = 0
        self.misses = 0
        self.evicted_blocks_count = 0
        self.user_requests = defaultdict(int)          # Number of requests per user
        self.user_request_sizes = defaultdict(int)     # Total size of requests per user
        self.user_total_time = defaultdict(float)      # Total time per user

        self.ssd_time = 0
        self.hdd_time = 0
        self.total_time = 0

        self.mis_block = 0
        self.hit_block = 0
        self.output_accumulator = ""
        self.reel_misses = 0
        self.read_times = 0
        self.write_times = 0
        self.migration_times = 0
        self.evicted_file_count = 0
        self.total_eviction_time = 0
        self.hdd_time_pref = 0
        self.ssd_time_evict = 0
        self.hdd_time_evict = 0
        self.nbr_of_blocks_hdd_reads = 0

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
                # Always send a full burst buffer size, even if remaining < burst_buffer_size
                burst = self.burst_buffer_size
                read_time += (burst / read_tier.read_throughput) + read_tier.latency
                remaining -= self.burst_buffer_size

        # Handle writing to write_tier
        if write_tier:
            remaining = data_size
            while remaining > 0:
                # Similarly for writing, always a full burst buffer size
                burst = self.burst_buffer_size
                write_time += (burst / write_tier.write_throughput) + write_tier.latency
                remaining -= self.burst_buffer_size

        return read_time, write_time

    def evict(self):
        """
        Evict a block from SSD to HDD using the LRU strategy.
        The least recently used block is selected for eviction.
        This eviction is not immediate; the block is moved to a list for later transfer.
        
        :return: A list containing the evicted block.
        """
        evicted_blocks = []
        if self.lru_list:
            # Pop the first item (least recently used)
            lru_key, _ = self.lru_list.popitem(last=False)
            # Remove block from SSD tier
            self.ssd_tier.remove_block(*lru_key)
            # Add block to HDD tier
            self.hdd_tier.add_block(*lru_key)
            # Update user space usage
            self.users[lru_key[0].user_id].decrease_space(1)
            # Increment eviction count
            self.evicted_blocks_count += 1
            evicted_blocks.append(lru_key)
        return evicted_blocks

    def log_user_times(self):
        """
        Log each user's throughput, total time, and space usage in the specified log file.
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
                    f"Total Time = {time_spent} s | "
                    f"Default Space = {space_default} | "
                    f"Used Space = {space_used}\n"
                )
            log_file.write("\n")

    def on_io(self, file, timestamp, requestType, offsetStart, offsetEnd):
        """
        Handle an IO operation on the given file from offsetStart to offsetEnd at the specified timestamp.
        This function processes hits/misses, evictions, and possible block movements between HDD and SSD.

        Final logic required:
        - SSD Hit: immediate SSD time.
        - Block on HDD on miss: immediate HDD->SSD time (HDD read + SSD write).
        - Block not present: immediate SSD time.
        - SSD->HDD Eviction: not immediate. Store evicted blocks and process them at the end.

        Follows the same pattern as ARC but with LRU.
        
        :param file: The File object being accessed.
        :param timestamp: The timestamp of the IO operation.
        :param requestType: The type of request (read/write).
        :param offsetStart: The starting block offset.
        :param offsetEnd: The ending block offset (non-inclusive).
        """
        # Reset time counters for this IO
        self.total_time = 0
        self.ssd_time = 0
        self.hdd_time = 0
        self.ssd_time_evict = 0
        self.hdd_time_evict = 0
        self.hdd_time_pref = 0
        self.migration_times = 0
        self.prefetch_times = 0

        user_id = file.user_id
        self.io_counter += 1

        request_size_in_blocks = (offsetEnd - offsetStart)
        request_size_in_bytes = request_size_in_blocks * self.block_size

        self.user_request_sizes[user_id] += request_size_in_bytes
        self.user_requests[user_id] += 1

        # New list to store all blocks evicted during THIS request
        evicted_blocks_during_request = []

        for block_offset in range(offsetStart, offsetEnd):
            block = (file, block_offset)

            if block in self.lru_list:
                # Hit SSD
                self.hits += 1
                # Move the accessed block to the end to mark it as recently used
                self.lru_list.move_to_end(block, last=True)
                # Immediate SSD read time
                self.ssd_time += (self.block_size / self.ssd_tier.read_throughput) + self.ssd_tier.latency
            else:
                # Miss
                self.misses += 1
                # If cache is full, evict the least recently used block
                while len(self.lru_list) >= self.c:
                    evicted_blocks = self.evict()
                    evicted_blocks_during_request.extend(evicted_blocks)

                if self.hdd_tier.is_block_in_file(*block):
                    # Block is on HDD: immediate HDD->SSD transfer
                    hdd_read_time = (self.block_size / self.hdd_tier.read_throughput) + self.hdd_tier.latency
                    ssd_write_time = (self.block_size / self.ssd_tier.write_throughput) + self.ssd_tier.latency
                    self.hdd_time += hdd_read_time
                    self.ssd_time += ssd_write_time

                    # Move block from HDD to SSD
                    self.hdd_tier.remove_block(*block)
                    self.ssd_tier.add_block(*block)
                    # Update user space usage
                    self.users[user_id].increase_space(1)
                    # Add block to LRU list as most recently used
                    self.lru_list[block] = None
                else:
                    # Block is nowhere: immediate SSD write
                    self.ssd_tier.add_block(*block)
                    self.users[user_id].increase_space(1)
                    self.lru_list[block] = None
                    # Immediate SSD write time
                    self.ssd_time += (self.block_size / self.ssd_tier.write_throughput) + self.ssd_tier.latency

        # After processing all blocks, handle the SSD->HDD evictions
        if evicted_blocks_during_request:
            # Calculate total evicted data size in bytes
            total_data_size = len(evicted_blocks_during_request) * self.block_size
            # Single call for all evicted blocks
            ssd_read_time, hdd_write_time = self._transfer_data_with_burst(
                total_data_size, self.ssd_tier, self.hdd_tier
            )
            self.ssd_time += ssd_read_time
            self.hdd_time += hdd_write_time

        # Calculate total time spent on this IO operation
        self.total_time += self.ssd_time + self.hdd_time

        # Update user total time
        self.user_total_time[user_id] += self.total_time

        # Update time spent for the user object
        self.users[user_id].increase_time_spent(self.total_time)

        # Debug print for verifying the IO counter
        print(self.io_counter)

        # If this IO is the last one, log user times
        if self.io_counter == self.total_io_operations:
            print("It's the last file")
            self.log_user_times()
            self.io_counter = 0
