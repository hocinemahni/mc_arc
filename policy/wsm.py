from collections import defaultdict, deque, OrderedDict
from structures.file import File
from policy.policy import Policy
import numpy as np

class BFH_wsm(Policy):
    def __init__(self, cache_size,users, alpha, ssd_tier, hdd_tier):

        Policy.__init__(self, cache_size, alpha, ssd_tier, hdd_tier)
        self.p = 0
        self.users = users
        self.mis_block = 0
        self.hit_block = 0
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
        self.hits_in_hdd_b1_b2 = 0
        # New hit and miss counters
        self.hits = 0
        self.misses = 0
        self.false_misses = 0
        self.reel_misses = 0
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
        self.adapte_B1 = 0
        self.adapte_B2 = 0
        self.beta = 1
        self.ssd_time = 0
        self.hdd_time = 0
        self.hdd_time_pref = 0
        self.ssd_time_evict = 0
        self.hdd_time_evict = 0
        self.nbr_of_blocks_hdd_reads = 0
    def t1_max_size(self) -> int:
        return self.p

    def t2_max_size(self) -> int:
        return self.c - self.p

    def b1_max_size(self) -> int:
        return self.c - self.p

    def b2_max_size(self) -> int:
        return self.p

    def evict(self):
        file2position_score = defaultdict(int)
        file2lifetime_score = defaultdict(float)
        file2fairness_score = defaultdict(float)

        # Calculer les scores basés sur la position
        for i, block in enumerate(self.t1):
            file, _ = block
            file2position_score[file] += len(self.t1) - i

        for j, block in enumerate(self.t2):
            file, _ = block
            file2position_score[file] += len(self.t2) - j

        # Diviser la somme des scores de position par la taille du fichier
        for file in file2position_score.keys():
            file2position_score[file] /= file.size

        # Calculer les scores basés sur la durée de vie restante
        for file in set(file2position_score.keys()):
            time_diff = (file.lifetime - (self.time_now - file.firstAccessTime)) / 1000  # Convert to seconds
            file2lifetime_score[file] = np.exp(-np.float64(time_diff))
            print("file2lifetime_score[file] = ", file2lifetime_score[file])
        # Calculer les scores basés sur l'équité
        for file in set(file2position_score.keys()):
            if self.users[file.user_id].space_default != 0:
                file2fairness_score[file] = (self.users[file.user_id].space_utilization - self.users[
                    file.user_id].space_default) / self.users[file.user_id].space_default
            else:
                file2fairness_score[file] = 1
            # Debug: Print the score dictionaries
        print("file2position_score:", file2position_score)
        # print("file2lifetime_score:", file2lifetime_score)
        print("file2fairness_score:", file2fairness_score)
        # Normaliser les scores et combiner
        max_position_score = max(file2position_score.values(), default=1)
        max_lifetime_score = max(file2lifetime_score.values(), default=1)
        max_fairness_score = max(file2fairness_score.values(), default=1)

        combined_scores = {}
        for file in file2position_score:
            normalized_position_score = file2position_score[file] / max_position_score if max_position_score != 0 else 0
            normalized_lifetime_score = file2lifetime_score[file] / max_lifetime_score if max_lifetime_score != 0 else 0
            normalized_fairness_score = file2fairness_score[file] / max_fairness_score if max_fairness_score != 0 else 0
            combined_scores[
                file] = 1 / 3 * normalized_position_score + 1 / 3 * normalized_lifetime_score + 1 / 3 * normalized_fairness_score
            print('normalized_position_score = ', normalized_position_score)
            print('normalized_lifetime_score = ', normalized_lifetime_score)
            print('normalized_fairness_score = ', normalized_fairness_score)
        
        # Trouver le fichier avec le score combiné le plus élevé
        worse_file = max(combined_scores, key=combined_scores.get, default=None)
        #worse_file = max(file2score, key=file2score.get)
        assert worse_file is not None
        assert worse_file.size > 0
        #print(worse_file.name)
        print('file to evict:', worse_file.name)
        self.ssd_tier.remove_file(worse_file.name)
        self.hdd_tier.add_file(worse_file)
        self.evicted_blocks_count += worse_file.size
        self.evicted_file_count += 1
        self.remove_all(worse_file)
        # calculating the time it takes to read the file from the ssd and write it to the hdd
        data_size_to_transfer = (worse_file.size * self.block_size)
        ssd_read_time = (data_size_to_transfer / self.ssd_tier.read_throughput) + self.ssd_tier.latency
        hdd_write_time = (data_size_to_transfer / self.hdd_tier.write_throughput) + self.hdd_tier.latency
        
        self.ssd_time += ssd_read_time
        self.hdd_time += hdd_write_time
        
        if (len(self.t1) + len(self.b1)) == self.c:
            if len(self.t1) < self.c:
                if len(self.b1) >= worse_file.size:
                    for _ in range(worse_file.size):
                        oldest_key2 = next(iter(self.b1))  # the first key from the dictionary
                        self.b1.pop(oldest_key2)
                else:
                    if len(self.b1) >= self.adapte_B1:
                        for _ in range(self.adapte_B1):

                            oldest_key = next(iter(self.b1))  # the first key from the dictionary
                            self.b1.pop(oldest_key)
                    else:
                        nombre_blocs_supprimes_b1 = len(self.b1)
                        self.b1.clear()
                        for _ in range(worse_file.size - nombre_blocs_supprimes_b1):
                            oldest_key2 = next(iter(self.b2))  # the first key from the dictionary
                            self.b2.pop(oldest_key2)
                    if len(self.b2) >= self.adapte_B2:
                        for _ in range(self.adapte_B2):
                            oldest_key2 = next(iter(self.b2))  # the first key from the dictionary
                            self.b2.pop(oldest_key2)
                    else:

                        nombre_blocs_supprimes_b2 = len(self.b2)
                        self.b2.clear()
                        for _ in range(worse_file.size - nombre_blocs_supprimes_b2):
                            oldest_key2 = next(iter(self.b1))  # the first key from the dictionary
                            self.b1.pop(oldest_key2)
        elif (len(self.t1) + len(self.t2) + len(self.b1) + len(self.b2)) >= self.c:
            if (len(self.t1) + len(self.t2) + len(self.b1) + len(self.b2)) == (self.c * 2):
                if len(self.b2) >= worse_file.size:
                    for _ in range(worse_file.size):
                        oldest_key2 = next(iter(self.b2))  # the first key from the dictionary
                        self.b2.pop(oldest_key2)
                else:
                    if len(self.b2) >= self.adapte_B2:
                        for _ in range(self.adapte_B2):
                            oldest_key2 = next(iter(self.b2))  # the first key from the dictionary
                            self.b2.pop(oldest_key2)
                    else:

                        nombre_blocs_supprimes_b2 = len(self.b2)
                        self.b2.clear()
                        for _ in range(worse_file.size - nombre_blocs_supprimes_b2):
                            oldest_key2 = next(iter(self.b1))  # the first key from the dictionary
                            self.b1.pop(oldest_key2)
                    if len(self.b1) >= self.adapte_B1:
                        for _ in range(self.adapte_B1):

                            oldest_key = next(iter(self.b1))  # the first key from the dictionary
                            self.b1.pop(oldest_key)
                    else:
                        nombre_blocs_supprimes_b1 = len(self.b1)
                        self.b1.clear()
                        for _ in range(worse_file.size - nombre_blocs_supprimes_b1):
                            oldest_key2 = next(iter(self.b2))  # the first key from the dictionary
                            self.b2.pop(oldest_key2)

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
        #self.ssd_tier.remove_file(file.name)

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

    def load_file_to(self, file, tier):
        ''' Load a file to the SSD tier
        
        Args:   
            file: File object to be loaded
            tier: Tier to which the file will be loaded'''
        # verify if the file can fit in the tier
        if file.size <= (self.c - (len(self.t1) + len(self.t2))):


            for block_offset in range(file.size):
                # A block is identified by its file and offset
                block = (file, block_offset)

                # We add the block to the file's block list
                self.file2blocks[file].add(block)

                # We add the block to t1's list
                tier[block] = None
            self.ssd_tier.add_file(file)
            self.file2tier[file] = 1
        else:
            self.evict()

    def move_file_to(self, file, tier):
        ''' Move a file to the SSD tier
        
        Args:
            file: File object to be moved
            tier: Tier to which the file will be moved'''
        self.remove_all_hard(file)
        self.load_file_to(file, tier)

    def is_filename_in_b1(self, file_name):
        for (file_obj, block_num) in self.b1.keys():
            if file_obj.name == file_name:
                return True
        return False

    def is_filename_in_b2(self, file_name):
        for (file_obj, block_num) in self.b2.keys():
            if file_obj.name == file_name:
                return True
        return False

    def on_io(self, file, timestamp, requestType, offsetStart, offsetEnd):
        self.ssd_time = 0
        self.time_now = timestamp
        self.hdd_time = 0
        self.ssd_time_evict = 0
        self.hdd_time_evict = 0
        self.hdd_time_pref = 0
        self.total_time = 0
        self.write_times = self.read_times = self.prefetch_times = self.migration_times = 0
        self.isinb2 = False
        self.hdd_time = 0
        new_file = False
        


        for block_offset in range(offsetStart, offsetEnd):
            block = (file, block_offset)
            # if block in self.t1 and (self.new_file == False) or block in self.t2:
            if (block in self.t1 and new_file is False and requestType != 'create') or block in self.t2:
            #if (block in self.t1 and new_file is False and requestType != 'PUT') or block in self.t2:
                self.hits += 1
            else:
                # Si le bloc n'est pas dans t1 ou t2, c'est un 'miss'
                self.misses += 1
            # Vérifier si le bloc est dans t1 ou t2
            if block in self.t1:
                if not new_file:
                    self.ssd_time += (self.block_size / self.ssd_tier.read_throughput) + self.ssd_tier.latency
                    # self.read_times += (self.block_size / self.ssd_tier.read_throughput)
                    del self.t1[block]
                    self.t2[block] = None
                else:
                    # self.read_times += (self.block_size / self.ssd_tier.read_throughput)
                    self.ssd_time += (self.block_size / self.ssd_tier.read_throughput) + self.ssd_tier.latency

            elif block in self.t2:
                self.ssd_time += (self.block_size / self.ssd_tier.read_throughput) + self.ssd_tier.latency
                # self.read_times += (self.block_size / self.ssd_tier.read_throughput)
                del self.t2[block]
                self.t2[block] = None  # Déplacer le bloc à la fin de T2 pour maintenir l'ordre d'accès
            elif block in self.b1:
                self.p = min(self.p + max(round(len(self.b2) / (len(self.b1))), file.size), self.c)
                self.move_file_to(file, self.t2)
                # self.hdd_cache.remove(file)
                self.hdd_tier.remove_file(file.name)
                #self.ssd_tier.add_file(file)
                self.hits_in_hdd_b1_b2 += 1
                # Calculer le temps nécessaire pour lire le fichier depuis le HDD
                self.hdd_time += ((file.size * self.block_size) / self.hdd_tier.read_throughput) + self.hdd_tier.latency

                # Calculer le temps nécessaire pour écrire le fichier sur le SSD
                self.ssd_time += ((
                                              file.size * self.block_size) / self.ssd_tier.write_throughput) + self.ssd_tier.latency

            elif block in self.b2:
                self.p = max(self.p - max(round(len(self.b1) / (len(self.b2))), file.size), 0)
                self.isinb2 = True
                self.move_file_to(file, self.t2)
                # self.hdd_cache.remove(file)
                self.hdd_tier.remove_file(file.name)
                #self.ssd_tier.add_file(file)
                self.hits_in_hdd_b1_b2 += 1
                # Calculer le temps nécessaire pour lire le fichier depuis le HDD
                self.hdd_time += ((file.size * self.block_size) / self.hdd_tier.read_throughput) + self.hdd_tier.latency
                # Calculer le temps nécessaire pour écrire le fichier sur le SSD
                self.ssd_time += ((
                                              file.size * self.block_size) / self.ssd_tier.write_throughput) + self.ssd_tier.latency
            # elif self.hdd_tier.is_file_in_tier(file.name) and not self.ssd_tier.is_file_in_tier(file.name):
            #     self.hdd_time += (self.block_size / self.hdd_tier.read_throughput) + self.hdd_tier.latency
            elif self.hdd_tier.is_file_in_tier(file.name) and not self.is_filename_in_b2(file.name) and not self.is_filename_in_b1(file.name):
                if file.size <= self.c:
                    new_file = True
                    self.hdd_tier.remove_file(file.name)
                    #self.ssd_tier.add_file(file)
                    self.false_misses += 1
                    self.remove_all_hard(file)
                    self.load_file_to(file, self.t1)
                    self.file2tier[file] = 1
                    # Calculer le temps nécessaire pour lire le fichier depuis le HDD
                    hdd_read_time = ((file.size * self.block_size) / self.hdd_tier.read_throughput) + \
                                    self.hdd_tier.latency

                    # Calculer le temps nécessaire pour écrire le fichier sur le SSD
                    ssd_write_time = ((file.size * self.block_size) / self.ssd_tier.write_throughput) + \
                                     self.ssd_tier.latency

                    # Si les opérations de lecture et d'écriture sont en parallèle,
                    # prendre le maximum des deux temps
                    # self.prefetch_times += max(hdd_read_time, ssd_write_time)
                    self.ssd_time += ssd_write_time
                    self.hdd_time += hdd_read_time
                if file.size >  self.c:
                    #print("file size", file.size, 'file name', file.name, 'file nest pas dans b1', file,
                         # 'est pas dans b2', 'et il est pas dans hdd', 'et il est plus grand que c', self.c)
                    #print(self.nbr_of_blocks_hdd_reads, 'blocks read from hdd')
                    self.hdd_time += (((offsetEnd - offsetStart) * self.block_size) / self.hdd_tier.read_throughput) + \
                                     self.hdd_tier.latency
                    self.nbr_of_blocks_hdd_reads += (offsetEnd - offsetStart)
            else :
                if not self.hdd_tier.is_file_in_tier(file.name) and not self.ssd_tier.is_file_in_tier(file.name) and not self.is_filename_in_b2(file.name) and not self.is_filename_in_b1(file.name):
                    if file.size <= self.c:
                        new_file = True
                        #self.ssd_tier.add_file(file)
                        self.false_misses += 1
                        self.load_file_to(file, self.t1)
                        self.file2tier[file] = 1
                        self.ssd_time += (((offsetEnd - offsetStart) * self.block_size) / self.ssd_tier.write_throughput) + \
                                         self.ssd_tier.latency
                    else:
                        '''print("file size", file.size, 'file name', file.name, 'file nest pas dans b1', file,
                              'est pas dans b2', 'et nest pas dans hdd', 'et il est plus grand que c', self.c)'''
                        #print(self.nbr_of_blocks_hdd_reads, 'blocks read from hdd')
                        self.hdd_tier.add_file(file)
                        self.hdd_time += (((offsetEnd - offsetStart) * self.block_size) / self.hdd_tier.read_throughput) + \
                                         self.hdd_tier.latency
                        self.nbr_of_blocks_hdd_reads += (offsetEnd - offsetStart)
        # Calcul du temps total

        self.total_time += self.ssd_time + self.hdd_time #+ self.migration_times
        print('hits', self.hits)
        print(self.nbr_of_blocks_hdd_reads, 'blocks read from hdd')
        print('misses', self.misses)
        #print('la taille de t1 et t2 :', len(self.t1) + len(self.t2))
        #print('la taille de b1 et b2 :', len(self.b1) + len(self.b2))
        # print(len(self.ssd_tier.files))
        # print((self.ssd_tier.size))
        # print((self.ssd_tier.files))
        # # print(len(self.file2blocks))
        # # print(self.t1)
        # # print(self.t2)
        # print(self.ssd_tier.__str__())
