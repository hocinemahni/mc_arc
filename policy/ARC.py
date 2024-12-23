# -*- coding: utf-8 -*-
from collections import defaultdict
from structures.file import File
from structures.deque import Deque
from .policy import Policy
import datetime

deques = Deque

class Arc_block_Cache(Policy):
    """
    Arc_block_Cache is a caching policy that manages blocks between SSD and HDD tiers.
    It uses the Adaptive Replacement Cache (ARC) algorithm with block-level management,
    incorporating a burst buffer mechanism for efficient data transfers.
    """

    def __init__(self, cache_size, users, alpha, ssd_tier, hdd_tier, log_file_path, total_io_count):
        """
        Initialize the Arc_block_Cache caching policy instance.

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
        self.user_requests = defaultdict(int)
        self.user_request_sizes = defaultdict(int)
        self.user_total_time = defaultdict(float)
        self.io_counter = 0
        self.hit_block = 0
        self.total_io_operations = total_io_count
        self.log_file_path = log_file_path
        self.mis_block = 0

        # Block size = 4KB
        self.block_size = 4 * 1024

        # Burst buffer size = 256KB
        self.burst_buffer_size = 256 * 1024

        self.nbr_of_blocks_hdd_reads = 0
        self.c = cache_size
        self.p = 0
        self.hits = 0
        self.misses = 0
        self.evicted_blocks_count = 0
        self.ssd_tier = ssd_tier
        self.hdd_tier = hdd_tier
        self.evicted_file_count = 0
        self.migration_times = 0
        self.hits_in_hdd_b1_b2 = 0
        self.read_times = 0
        self.write_times = 0
        self.total_time = 0
        self.prefetch_times = 0

        # Initialize cache structures using Deque for T1, T2, B1, B2
        self.t1 = deques()
        self.b1 = deques()
        self.t2 = deques()
        self.b2 = deques()

        # Time tracking for SSD and HDD operations
        self.ssd_time = 0
        self.hdd_time = 0
        self.hdd_time_pref = 0
        self.ssd_time_evict = 0
        self.hdd_time_evict = 0

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

    def replace(self, args):
        """
        Replace a block in the cache based on the ARC algorithm.
        This method determines which block to evict and performs the eviction.

        :param args: A tuple representing the block (file, block_offset) to be accessed.
        :return: A list of evicted blocks.
        """
        evicted_blocks = []
        if self.t1 and ((args in self.b2 and len(self.t1) == self.p) or (len(self.t1) > self.p)):
            old = self.t1.pop()
            evicted_blocks.append(old)
            self.b1.appendleft(old)
            self.ssd_tier.remove_block(*old)
            self.hdd_tier.add_block(*old)
            self.users[old[0].user_id].decrease_space(1)
        else:
            old = self.t2.pop()
            evicted_blocks.append(old)
            self.b2.appendleft(old)
            self.ssd_tier.remove_block(*old)
            self.hdd_tier.add_block(*old)
            self.users[old[0].user_id].decrease_space(1)

        self.evicted_blocks_count += len(evicted_blocks) + 1
        # print('evicted block', len(evicted_blocks))
        # print('evicted block count', self.evicted_blocks_count)
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
                throughput = self.user_request_sizes[user_id] / (self.user_total_time[user_id]) if self.user_total_time[user_id] > 0 else 0
                log_file.write(f"User {user_id}: Throughput = {throughput:.2f} bytes/sec | Total Time = {time_spent} s | Default Space = {space_default} | Used Space = {space_used}\n")
            log_file.write("\n")

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
        # Initialization
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
            args = (file, block_offset)

            # Hit/Miss
            if (args in self.t1) or args in self.t2:
                self.hits += 1
            else:
                self.misses += 1

            if args in self.t1:
                self.t1.remove(args)
                self.t2.appendleft(args)
                # SSD read 1 block
                self.ssd_time += (self.block_size / self.ssd_tier.read_throughput) + self.ssd_tier.latency

            elif args in self.t2:
                self.t2.remove(args)
                self.t2.appendleft(args)
                # SSD read 1 block
                self.ssd_time += (self.block_size / self.ssd_tier.read_throughput) + self.ssd_tier.latency

            elif args in self.b1:
                self.p = min(self.c, self.p + max(round(len(self.b2) / len(self.b1)), 1))
                # replace() now returns the evicted blocks, we store them
                evicted_blocks = self.replace(args)
                evicted_blocks_during_request.extend(evicted_blocks)
                self.ssd_time += (self.block_size / self.ssd_tier.write_throughput) + self.ssd_tier.latency
                self.hdd_time += (self.block_size / self.ssd_tier.read_throughput) + self.ssd_tier.latency
                self.b1.remove(args)
                self.t2.appendleft(args)
                self.ssd_tier.add_block(*args)
                self.hdd_tier.remove_block(*args)
                self.users[file.user_id].increase_space(1)

                # HDD->SSD transfer for 1 block
                # Previously calculated here, but now will be done AFTER the loop, globally.
                self.hits_in_hdd_b1_b2 += 1

            elif args in self.b2:
                self.p = max(0, self.p - max(round(len(self.b1) / len(self.b2)), 1))
                evicted_blocks = self.replace(args)
                evicted_blocks_during_request.extend(evicted_blocks)
                self.ssd_time += (self.block_size / self.ssd_tier.write_throughput) + self.ssd_tier.latency
                self.hdd_time += (self.block_size / self.ssd_tier.read_throughput) + self.ssd_tier.latency
                self.b2.remove(args)
                self.t2.appendleft(args)
                self.ssd_tier.add_block(*args)
                self.hdd_tier.remove_block(*args)
                self.users[file.user_id].increase_space(1)

                self.hits_in_hdd_b1_b2 += 1

            else:
                # Block is nowhere
                if len(self.t1) + len(self.b1) == self.c:
                    if len(self.t1) < self.c:
                        self.b1.pop()
                        evicted_blocks = self.replace(args)
                        evicted_blocks_during_request.extend(evicted_blocks)
                    else:
                        if self.t1:
                            t1pop = self.t1.pop()
                            self.users[t1pop[0].user_id].decrease_space(1)
                            self.ssd_tier.remove_block(*t1pop)
                            self.evicted_blocks_count += 1
                            # Eviction SSD->HDD for 1 block
                            # Similarly: add this evicted block to the list for global transfer
                            evicted_blocks_during_request.append(t1pop)

                else:
                    total = len(self.t1) + len(self.b1) + len(self.t2) + len(self.b2)
                    if total >= self.c:
                        if total == (2 * self.c):
                            if self.b2:
                                self.b2.pop()
                        evicted_blocks = self.replace(args)
                        evicted_blocks_during_request.extend(evicted_blocks)

                if self.hdd_tier.is_block_in_file(*args) and args not in self.b1 and args not in self.b2:
                    if (len(self.t1) + len(self.t2)) <= self.c:
                        self.evicted_blocks_count += 1
                        self.hdd_tier.remove_block(*args)
                        self.ssd_tier.add_block(*args)
                        self.t1.appendleft(args)
                        self.users[file.user_id].increase_space(1)
                        self.hdd_time += (self.block_size / self.hdd_tier.read_throughput) + self.hdd_tier.latency
                        self.ssd_time += (self.block_size / self.ssd_tier.write_throughput) + self.ssd_tier.latency
                        # Previously, we transferred here. Now we do not.
                    else:
                        evicted_blocks = self.replace(args)
                        evicted_blocks_during_request.extend(evicted_blocks)
                else:
                    if self.c < 1 and args not in self.b1 and args not in self.b2:
                        # Write to HDD (no SSD->HDD eviction here, just add to HDD)
                        self.hdd_tier.add_block(*args)
                        self.users[args[0].user_id].decrease_space(args[0].size)
                        # No eviction to perform here, just direct HDD access
                        self.hdd_time += (self.block_size / self.hdd_tier.read_throughput) + self.hdd_tier.latency
                    else:
                        if (len(self.t1) + len(self.t2) <= self.c) and args not in self.b1 and args not in self.b2:
                            self.t1.appendleft(args)
                            self.ssd_tier.add_block(*args)
                            self.ssd_time += (self.block_size / self.ssd_tier.write_throughput) + self.ssd_tier.latency
                            self.users[file.user_id].increase_space(1)
                        else:
                            if (len(self.t1) + len(self.t2) > self.c) and args not in self.b1 and args not in self.b2:
                                evicted_blocks = self.replace(args)
                                evicted_blocks_during_request.extend(evicted_blocks)
                                self.ssd_tier.add_block(*args)
                                self.ssd_time += (self.block_size / self.ssd_tier.write_throughput) + self.ssd_tier.latency
                                self.users[file.user_id].increase_space(1)
                            evicted_blocks = self.replace(args)
                            evicted_blocks_during_request.extend(evicted_blocks)
                            self.ssd_tier.add_block(*args)
                            self.ssd_time += (self.block_size / self.ssd_tier.write_throughput) + self.ssd_tier.latency
                            self.users[file.user_id].increase_space(1)

        # Once all blocks are processed, perform the aggregated transfer for all evicted blocks
            if evicted_blocks_during_request:
                # Calculate total evicted data size in bytes
                total_data_size = (len(evicted_blocks_during_request)) * self.block_size
                # Single call for all evicted blocks
                ssd_read_time, hdd_write_time = self._transfer_data_with_burst(total_data_size, self.ssd_tier, self.hdd_tier)
                self.ssd_time += ssd_read_time
                self.hdd_time += hdd_write_time

            # Calculate total time spent on this IO operation
            self.total_time += self.ssd_time + self.hdd_time

            # Update user total time
            self.user_total_time[user_id] += self.total_time

            # Update time spent for the user object
            self.users[file.user_id].increase_time_spent(self.total_time)

            # Debug print for verifying the IO counter
            print(self.io_counter)

            # If this IO is the last one, log user times
            if self.io_counter == self.total_io_operations:
                print("f It's the last file")
                self.log_user_times()
                self.io_counter = 0
