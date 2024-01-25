#from collections import defaultdict, deque
from collections import OrderedDict, defaultdict, deque
from structures.file import File
from .policy import Policy
import math


class ARC_File_Policyv1(Policy):
    def __init__(self, cache_size, alpha, ssd_tier, hdd_tier):

        Policy.__init__(self, cache_size, alpha, ssd_tier, hdd_tier)
        self.p = 0

        self.c = cache_size
        self.alpha = alpha
        self.block_size = 1024
        self.ssd_tier = ssd_tier
        self.hdd_tier = hdd_tier
        self.t1 = OrderedDict()
        self.t2 = OrderedDict()
        self.b1 = OrderedDict()
        self.b2 = OrderedDict()
        self.output_accumulator = ""
        self.ssd_cache = deque(maxlen=cache_size)
        self.hdd_cache = deque(maxlen=cache_size)
        self.hits_in_hdd_b1_b2 = 0  # Nouvelle métrique pour suivre les hits dans b1 et b2
        # New hit and miss counters
        self.hits = 0
        self.hit_block = 0
        self.miss_block = 0
        self.misses = 0
        self.false_misses = 0
        self.read_times = 0
        self.write_times = 0
        self.total_time = 0
        self.prefetch_times = 0
        self.migration_times_evict = 0
        self.total_eviction_time = 0
        # nombres de blocks évincé
        self.evicted_blocks_count = 0
        self.evicted_file_count = 0
        self.file2blocks = defaultdict(set)
        self.file2tier = defaultdict(int)
        self.migration_times = 0
        self.isinb2 = False
        self.ssd_time = 0
        self.hdd_time = 0
        self.hdd_time_pref = 0
        self.ssd_time_evict =0
        self.hdd_time_evict = 0

    def t1_max_size(self) -> int:
        return self.p

    def t2_max_size(self) -> int:
        return self.c - self.p

    def b1_max_size(self) -> int:
        return self.c - self.p

    def b2_max_size(self) -> int:
        return self.p

    def evict(self):
        file_to_evict = None
        if self.t1 and ((self.isinb2 and len(self.t1) == self.p) or (len(self.t1) > self.p)):

            file_to_evict, _ = self.t1.popitem(last=False)  # Retire l'élément LRU de t1
            self.remove_all(file_to_evict[0])
            self.hdd_tier.add_file(file_to_evict[0])
            self.ssd_tier.remove_file(file_to_evict[0].name)
            # self.evicted_blocks_count += len(self.b1) + len(self.b2)
            self.evicted_blocks_count += file_to_evict[0].size
            self.evicted_file_count += 1

            # Calculer la taille des données à transférer en octets

            data_size_to_transfer = (file_to_evict[0].size * self.block_size)
            self.ssd_time_evict += (data_size_to_transfer / self.ssd_tier.read_throughput) + self.ssd_tier.latency
            self.hdd_time_evict += (data_size_to_transfer / self.hdd_tier.write_throughput) + self.hdd_tier.latency
            #ssd_read_time = data_size_to_transfer / self.ssd_tier.read_throughput
            #hdd_write_time = data_size_to_transfer / self.hdd_tier.write_throughput
            #max_transfer_time = max(ssd_read_time, hdd_write_time)
            # Calculer le temps de migration
            #self.migration_times += max_transfer_time + self.ssd_tier.latency + self.hdd_tier.latency
            if len(self.b1) >= file_to_evict[0].size:
                for j in range(file_to_evict[0].size):
                    self.b1.popitem(last=False)
                self.b1.clear()
            else:
                nombre_blocs_supprimes_b1 = len(self.b1)
                self.b1.clear()
                for _ in range(file_to_evict[0].size - nombre_blocs_supprimes_b1):
                    if self.b2:
                        self.b2.popitem(last=False)
                # self.b2.clear()

                if len(self.b2) >= file_to_evict[0].size:
                    for j in range(file_to_evict[0].size):
                        self.b2.popitem(last=False)
                    # self.b2.clear()
                else:

                    nombre_blocs_supprimes_b1 = len(self.b2)
                    self.b2.clear()
                    if self.b1:
                      for _ in range(file_to_evict[0].size - nombre_blocs_supprimes_b1):
                        # print('hello 2', _)
                        self.b1.popitem(last=False)
            #self.b1[file_to_evict] = None

        else:
            if self.t2:
                file_to_evict, _ = self.t2.popitem(last=False)  # Retire l'élément LRU de t2
                

                # Calculer la taille des données à transférer en octets
                data_size_to_transfer = (file_to_evict[0].size * self.block_size)
                self.ssd_time_evict += (data_size_to_transfer / self.ssd_tier.read_throughput) + self.ssd_tier.latency
                self.hdd_time_evict += (data_size_to_transfer / self.hdd_tier.write_throughput) + self.hdd_tier.latency
                #self.ssd_time += (data_size_to_transfer / self.ssd_tier.read_throughput)
                #ssd_read_time = data_size_to_transfer / self.ssd_tier.read_throughput
                #hdd_write_time = data_size_to_transfer / self.hdd_tier.write_throughput
                #max_transfer_time = max(ssd_read_time, hdd_write_time)
                # Calculer le temps de migration
                #self.migration_times += max_transfer_time + self.ssd_tier.latency + self.hdd_tier.latency
                self.remove_all(file_to_evict[0])
                self.ssd_tier.remove_file(file_to_evict[0].name)
                self.hdd_tier.add_file(file_to_evict[0])

                # self.evicted_blocks_count += len(self.b1) + len(self.b2)
                self.evicted_blocks_count += file_to_evict[0].size
                self.evicted_file_count += 1

                if len(self.b1) >= file_to_evict[0].size:
                    for j in range(file_to_evict[0].size):
                        self.b1.popitem(last=False)
                    self.b1.clear()
                else:
                    nombre_blocs_supprimes_b1 = len(self.b1)
                    self.b1.clear()
                    for _ in range(file_to_evict[0].size - nombre_blocs_supprimes_b1):
                        if self.b2:
                            self.b2.popitem(last=False)
                    # self.b2.clear()

                    if len(self.b2) >= file_to_evict[0].size:
                        for j in range(file_to_evict[0].size):
                            self.b2.popitem(last=False)
                        # self.b2.clear()
                    else:
                        nombre_blocs_supprimes_b1 = len(self.b2)
                        self.b2.clear()
                        for _ in range(file_to_evict[0].size - nombre_blocs_supprimes_b1):
                            # print('hello 2', _)
                            if self.b1:
                               self.b1.popitem(last=False)
            else: 
                if self.t1:
                    file_to_evict, _ = self.t1.popitem(last=False)  # Retire l'élément LRU de t2
                    self.remove_all(file_to_evict[0])
                    self.ssd_tier.remove_file(file_to_evict[0].name)
                    self.hdd_tier.add_file(file_to_evict[0])

                    # self.evicted_blocks_count += len(self.b1) + len(self.b2)
                    self.evicted_blocks_count += file_to_evict[0].size
                    self.evicted_file_count += 1

                    # Calculer la taille des données à transférer en octets
                    data_size_to_transfer = (file_to_evict[0].size * self.block_size)
                    self.ssd_time_evict += (data_size_to_transfer / self.ssd_tier.read_throughput) + self.ssd_tier.latency
                    self.hdd_time_evict += (data_size_to_transfer / self.hdd_tier.write_throughput) + self.hdd_tier.latency
                    #self.ssd_time += (data_size_to_transfer / self.ssd_tier.read_throughput)
                    #ssd_read_time = data_size_to_transfer / self.ssd_tier.read_throughput
                    #hdd_write_time = data_size_to_transfer / self.hdd_tier.write_throughput
                    ######max_transfer_time = max(ssd_read_time, hdd_write_time)
                    # Calculer le temps de migration
                    #self.migration_times += max_transfer_time + self.ssd_tier.latency + self.hdd_tier.latency


                    if len(self.b1) >= file_to_evict[0].size :
                        for j in range(file_to_evict[0].size ):
                            self.b1.popitem(last=False)
                        self.b1.clear()
                    else:
                        nombre_blocs_supprimes_b1 = len(self.b1)
                        self.b1.clear()
                        for _ in range(file_to_evict[0].size  - nombre_blocs_supprimes_b1):
                            if self.b2:
                               self.b2.popitem(last=False)
                        #self.b2.clear()

                        if len(self.b2) >= file_to_evict[0].size :
                            for j in range(file_to_evict[0].size ):
                               self.b2.popitem(last=False)
                            #self.b2.clear()
                        else:
                            nombre_blocs_supprimes_b1 = len(self.b2)
                            self.b2.clear()
                            for _ in range(file_to_evict[0].size - nombre_blocs_supprimes_b1):
                                # print('hello 2', _)
                                self.b1.popitem(last=False)
                            #self.b1.clear()
            #self.b2[file_to_evict] = None

        # Déplacer le fichier vers le HDD

        #if file_to_evict is None:
            # No files with scores, so nothing to evict
         #   return


    def remove_all(self, file: File):
        """
        Remove all blocks of a file that are in t1 or t2, and add them to b1 and b2, respectively
        """

        # logging.debug(f'File {file} marked for unload. State before unload:')
        # logging.debug(self)
        blocks_t1 = [block for block in self.t1 if block[0] == file]
        blocks_t2 = [block for block in self.t2 if block[0] == file]

        for block in blocks_t1:
            del self.t1[block]
            self.b1[block] = None

        for block in blocks_t2:
            del self.t2[block]
            self.b2[block] = None

        self.file2tier[file] = 0
        del self.file2blocks[file]

    def remove_all_hard(self, file: File):
        """
        Remove all blocks of a file from t1, t2, b1 or b2
        """

        # logging.debug(f'File {file} marked for unload. State before unload:')
        # logging.debug(self)
        blocks = self.file2blocks[file]
        for block in blocks:
            if block in self.t1.keys():
                del self.t1[block]
            elif block in self.t2.keys():
                del self.t2[block]
            elif block in self.b1.keys():
                del self.b1[block]
            elif block in self.b2.keys():
                del self.b2[block]
        del self.file2blocks[file]
        self.file2tier[file] = 0

    """def load_file_to(self, file, tier):

        '''if file.size <= (self.c - (len(self.t1) + len(self.t2))):
            for block_offset in range(file.size):
                # A block is identified by its file and offset
                block = (file, block_offset)

                # We add the block to the file's block list
                self.file2blocks[file].add(block)

                # We add the block to t1's list
                tier[block] = None
        else:
            self.evict()'''
        while file.size >= (self.c - (len(self.t1) + len(self.t2))):
            self.evict()
        for block_offset in range(file.size):
                # A block is identified by its file and offset
                block = (file, block_offset)

                # We add the block to the file's block list
                self.file2blocks[file].add(block)

                # We add the block to t1's list
                tier[block] = None"""
    def load_file_to(self, file, tier):

        # if file.size > self.c:
        #      self.hdd_tier.add_file(file)
        #      #self.write_times += ((file.size * self.block_size) / self.hdd_tier.write_throughput)
        #      self.hdd_time += ((file.size * self.block_size )/ self.hdd_tier.write_throughput) + self.hdd_tier.latency
        #      #return #raise ValueError("taille du cache trop petite")

        space_needed = file.size
        space_available = self.c - (len(self.t1) + len(self.t2))
        while space_needed >= space_available:
            self.evict()
            space_available = self.c - (len(self.t1) + len(self.t2))
        for block_offset in range(file.size):
                # A block is identified by its file and offset
                block = (file, block_offset)

                # We add the block to the file's block list
                self.file2blocks[file].add(block)

                # We add the block to t1's list
                tier[block] = None

    def load_file_to2(self, file, tier):
        #self.migration_times = 0
        
        if len(self.t1) + len(self.b1) == self.c:
            if len(self.t1) < self.c:
                if len(self.b1) >= file.size:
                    for j in range(file.size):
                        self.b1.popitem(last=False)
                else:
                    nombre_blocs_supprimes_b1 = len(self.b1)
                    self.b1.clear()
                    for _ in range(file.size - nombre_blocs_supprimes_b1):
                        if self.b2:    
                           self.b2.popitem(last=False)

                self.evict()
            # if file.size > (self.c - (len(self.t1) + len(self.t2))):
            #     self.evict()

            else:
                #while file.size <= (self.c - (len(self.t1) + len(self.t2))):
                    file_to_evict, _ = self.t1.popitem(last=False)  # Retire l'élément LRU de t1
                    self.remove_all(file_to_evict[0])
                    self.evicted_blocks_count += file_to_evict[0].size
                    self.evicted_file_count += 1
                    self.ssd_tier.remove_file(file_to_evict[0].name)
                    self.hdd_tier.add_file(file)
                    # Déplacer le fichier vers le HDD
                    data_size_to_transfer = (file_to_evict[0].size * self.block_size)
                    self.ssd_time_evict += (data_size_to_transfer / self.ssd_tier.read_throughput) + self.ssd_tier.latency
                    self.hdd_time_evict += (data_size_to_transfer / self.hdd_tier.read_throughput) + self.hdd_tier.latency
                    #self.ssd_time_evict += (data_size_to_transfer / self.ssd_tier.read_throughput)
                    #ssd_read_time = (data_size_to_transfer / self.ssd_tier.read_throughput)
                    #hdd_write_time = (data_size_to_transfer / self.hdd_tier.write_throughput)
                    #max_transfer_time = max(ssd_read_time, hdd_write_time)
                    # Calculer le temps de migration
                    ##self.migration_times += (max_transfer_time + self.ssd_tier.latency + self.hdd_tier.latency)
                    if len(self.t1) + len(self.b1) == self.c:
                        if len(self.t1) < self.c:
                            if len(self.b1) >= file_to_evict[0].size :
                                for j in range(file_to_evict[0].size ):
                                    self.b1.popitem(last=False)
                            else:
                                nombre_blocs_supprimes_b1 = len(self.b1)
                                self.b1.clear()
                                for _ in range(file_to_evict[0].size  - nombre_blocs_supprimes_b1):
                                    if self.b2:    
                                       self.b2.popitem(last=False)
                            if len(self.b2) + len(self.b1) > self.c:
                                if len(self.b2) > len(self.b1):
                                    self.b2.clear()
                                else :
                                    self.b1.clear()               
                    else:
                        total = len(self.t1) + len(self.t2) + len(self.b1) + len(self.b2)
                        if total >= self.c:
                            if total == (self.c * 2):
                                if len(self.b2) >= file_to_evict[0].size :
                                    for j in range(file_to_evict[0].size ):
                                       self.b2.popitem(last=False)
                                else:
                                    nombre_blocs_supprimes_b1 = len(self.b2)
                                    self.b2.clear()
                                    for _ in range(file_to_evict[0].size - nombre_blocs_supprimes_b1):
                                        # print('hello 2', _)
        
                                        self.b1.popitem(last=False) 
                            if len(self.b2) + len(self.b1) > self.c:
                                if len(self.b2) > len(self.b1):
                                    self.b2.clear()
                                else :
                                    self.b1.clear()              
        else:           
            total = len(self.t1) + len(self.t2) + len(self.b1) + len(self.b2)
            if total >= self.c:
                if total == (self.c * 2):
                    if len(self.b2) >= file.size:
                        for j in range(file.size):
                           self.b2.popitem(last=False)
                    else:
                        nombre_blocs_supprimes_b1 = len(self.b2)
                        self.b2.clear()
                        for _ in range(file.size - nombre_blocs_supprimes_b1):
                            # print('hello 2', _)
                            self.b1.popitem(last=False)
                     
                #while file.size <= (self.c - (len(self.t1) + len(self.t2))):
                self.evict()
        #if file.size >= (self.c - (len(self.t1) + len(self.t2))) and file.size < self.c:
            #while file.size >= (self.c - (len(self.t1) + len(self.t2))):
                 #self.evict()
            if len(self.b2) + len(self.b1) > self.c:
                if len(self.b2) > len(self.b1):
                    self.b2.clear()
                else :
                    self.b1.clear()    
            #self.evict()
        if file.size > self.c:
            self.hdd_tier.add_file(file)
            self.write_times += ((file.size * self.block_size) / self.hdd_tier.write_throughput)
            self.hdd_time += ((file.size * self.block_size )/ self.hdd_tier.write_throughput) + self.hdd_tier.latency
        else:
            if file.size < (self.c - (len(self.t1) + len(self.t2))):
                for block_offset in range(file.size):
                    # A block is identified by its file and offset
                    block = (file, block_offset)

                    # We add the block to the file's block list
                    self.file2blocks[file].add(block)

                    # We add the block to t1's list
                    tier[block] = None


            while file.size >= (self.c - (len(self.t1) + len(self.t2))):
                self.evict()

            for block_offset in range(file.size):
                # A block is identified by its file and offset
                block = (file, block_offset)

                # We add the block to the file's block list
                self.file2blocks[file].add(block)

                # We add the block to t1's list
                tier[block] = None
            #if file.size >= (self.c - (len(self.t1) + len(self.t2))):
                #self.evict()

    def move_file_to(self, file, tier):
        self.remove_all_hard(file)
        self.load_file_to2(file, tier)

    def on_io(self, file, timestamp, requestType, offsetStart, offsetEnd):
        #io_blocks = {(file, offsetStart + i) for i in range(offsetEnd - offsetStart)}
        self.total_time = 0
        self.write_times = 0
        self.read_times = 0
        self.prefetch_times = 0
        self.isinb2 = False
        self.ssd_time = 0
        self.hdd_time = 0
        self.hdd_time_pref = 0
        self.ssd_time_evict = 0
        self.hdd_time_evict = 0
        # If no block from this file is in the cache, we want to load all the blocks
        #new_file = False
        new_file = False
        if not self.file2blocks[file]:
            if self.hdd_tier.is_file_in_tier(file.name):
                self.hdd_time += ((file.size) * self.block_size / self.hdd_tier.read_throughput) + self.hdd_tier.latency
                self.hdd_tier.remove_file(file.name)
                self.ssd_tier.add_file(file)
                # print(f'File {file} is not in cache, loading in t1.')
                new_file = True
                self.false_misses += 1
                # print(file.size)
                self.load_file_to2(file, self.t1)
                self.file2tier[file] = 1
            else:
                if file.size > self.c:
                    new_file = True
                    self.hdd_tier.add_file(file)
                    self.hdd_time += ((
                                          file.size) * self.block_size / self.hdd_tier.write_throughput) + self.hdd_tier.latency
                    self.write_times += ((file.size * self.block_size) / self.hdd_tier.write_throughput)
                else:
                    # print(f'File {file} is not in cache, loading in t1.')
                    new_file = True
                    self.false_misses += 1
                    # print(file.size)
                    self.load_file_to2(file, self.t1)
                    self.ssd_time += ((
                                              file.size * self.block_size) / self.ssd_tier.write_throughput) + self.ssd_tier.latency
                    self.write_times += ((
                                                 file.size * self.block_size) / self.ssd_tier.write_throughput) + self.ssd_tier.latency  # lat read
                    self.file2tier[file] = 1
                    self.ssd_tier.add_file(file)
        # if not self.file2blocks[file]:
        #     if self.hdd_tier.is_file_in_tier(file.name):
        #         self.hdd_time += ((file.size * self.block_size )/ self.hdd_tier.read_throughput) + self.hdd_tier.latency
        #         self.hdd_tier.remove_file(file.name)
        #         self.ssd_tier.add_file(file)
        #
        #         # print(f'File {file.size} is not in cache, loading in t1.')
        #         new_file = True
        #         self.false_misses += 1
        #         #self.remove_all_hard(file)
        #         self.load_file_to(file, self.t1)
        #         self.file2tier[file] = 1
        #     else:
        #         if file.size > self.c:
        #             new_file = True
        #
        #         else:
        #             new_file = True
        #             self.false_misses += 1
        #             self.ssd_tier.add_file(file)
        #             #self.remove_all_hard(file)
        #             self.load_file_to(file, self.t1)
        #             self.ssd_time += ((file.size * self.block_size) / self.ssd_tier.write_throughput) + self.ssd_tier.latency
        #             #self.write_times += ((file.size * self.block_size) / self.ssd_tier.write_throughput)
        #             self.file2tier[file] = 1
        #             # Calculer le temps nécessaire pour écrire le fichier sur le SSD
                    
                

        # Then, we do the I/O as usual
        for block_offset in range(offsetStart, offsetEnd):

            # A block is identified by its file and offset
            block = (file, block_offset)
            if block in self.t1 and (new_file ==False) or block in self.t2:
                #hits = True
                self.hits += 1
            else:
                # Si le bloc n'est pas dans t1 ou t2, c'est un 'miss'
                #hits = False
                self.misses += 1
            if block in self.t1.keys():
                if not new_file:
                    #self.read_times += (self.block_size / self.ssd_tier.read_throughput)
                    self.ssd_time += (self.block_size / self.ssd_tier.read_throughput) + self.ssd_tier.latency
                    blocks_t1 = [block for block in self.t1 if block[0] == file]

                    for block in blocks_t1:
                        del self.t1[block]
                        self.t2[block] = None
                else:
                    #self.read_times += (self.block_size / self.ssd_tier.read_throughput)
                    self.ssd_time += ((file.size * self.block_size) / self.ssd_tier.read_throughput) + self.ssd_tier.latency

            # If a block is in T2 and is accessed, we move this single block to the top of T2
            elif block in self.t2.keys():
                self.hit_block += 1
                self.ssd_time += (self.block_size / self.ssd_tier.read_throughput) + self.ssd_tier.latency
                #self.read_times += self.block_size / self.ssd_tier.read_throughput  # lat read
                blocks_t2 = [block for block in self.t2 if block[0] == file]

                for block in blocks_t2:
                    del self.t2[block]
                    self.t2[block] = None

            # If an evicted file's block is in B1 and is accessed, we move all its blocks to T2
            elif block in self.b1.keys():
                self.miss_block += 1
                self.p = min(self.p + max(round(len(self.b2) / (len(self.b1))), file.size), self.c)
                self.move_file_to(file, self.t2)

                self.hdd_tier.remove_file(file.name)

                self.ssd_tier.add_file(file)
                self.hits_in_hdd_b1_b2 += 1
                # Calculer le temps nécessaire pour lire le fichier depuis le HDD
                #hdd_read_time = ((file.size * self.block_size) / self.hdd_tier.read_throughput)

                # Calculer le temps nécessaire pour écrire le fichier sur le SSD
                #ssd_write_time = ((file.size * self.block_size) / self.ssd_tier.write_throughput)

                # Si les opérations de lecture et d'écriture sont en parallèle,
                # prendre le maximum des deux temps
                #max_transfer_time = max(hdd_read_time, ssd_write_time)

                # Additionner le temps de transfert maximum et les latences des deux tiers
                #total_prefetch_time = (max_transfer_time + self.ssd_tier.latency + self.hdd_tier.latency)

                # Mettre à jour le temps total de préchargement
                #self.prefetch_times += total_prefetch_time
                self.hdd_time_pref += ((file.size * self.block_size) / self.hdd_tier.read_throughput) + self.hdd_tier.latency
            # Same here, but with B2
            elif block in self.b2.keys():
                self.miss_block += 1
                self.isinb2 = True
                self.p = max(self.p - max(round(len(self.b1) / (len(self.b2))), file.size), 0)
                self.move_file_to(file, self.t2)
                self.hdd_tier.remove_file(file.name)

                self.ssd_tier.add_file(file)
                self.hits_in_hdd_b1_b2 += 1
                # Calculer le temps nécessaire pour lire le fichier depuis le HDD
                #hdd_read_time = ((file.size * self.block_size) / self.hdd_tier.read_throughput)

                # Calculer le temps nécessaire pour écrire le fichier sur le SSD
                #ssd_write_time = ((file.size * self.block_size) / self.sssd_tier.write_throughput)

                # Si les opérations de lecture et d'écriture sont en parallèle,
                # prendre le maximum des deux temps
                #max_transfer_time = max(hdd_read_time, ssd_write_time)

                # Additionner le temps de transfert maximum et les latences des deux tiers
                #total_prefetch_time = (max_transfer_time + self.ssd_tier.latency + self.hdd_tier.latency)

                # Mettre à jour le temps total de préchargement
                #self.prefetch_times += total_prefetch_time
                self.hdd_time_pref += ((file.size * self.block_size) / self.hdd_tier.read_throughput) + self.hdd_tier.latency

            #######print('nbr hit v2 %s', self.hits)
            #######print('nbr miss v2 %s', self.misses)
        self.reel_misses = self.misses - self.false_misses
        ######print(f'les varis miss sont : {self.reel_misses}, les faux misses : {self.false_misses}, le total est : {self.misses}')
        ######print(f' hits_on b1_b2: {self.hits_in_hdd_b1_b2}')
        self.total_time = self.ssd_time + self.hdd_time + self.hdd_time_pref + self.ssd_time_evict + self.hdd_time_evict
        #self.total_time = self.ssd_time + self.hdd_time + self.hdd_time_pref + self.ssd_time_evict + self.hdd_time_evict
        #self.total_time = (self.migration_times + self.prefetch_times + self.read_times + self.write_times)
        #print(f'total time v1, {self.total_time}')
        print('nbr hit v1 %s', self.hits)
        print('nbr miss v1 %s', self.misses)
        print('taille du cache  %s', self.c)
        print('la taille de t1 et t2 :',len(self.t1) + len(self.t2))
        print('la taille de b1 et b2 :',len(self.b1) + len(self.b2))
        print('nombre de fichiers evincés',self.evicted_file_count)
        print('nombre de blocks evincés ', self.evicted_blocks_count)
