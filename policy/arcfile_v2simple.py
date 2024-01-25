from collections import defaultdict, deque
from structures.file import File
from .policy import Policy
import math
class ARC_File_Policyv2simpl(Policy):
    def __init__(self, cache_size, alpha,  ssd_tier, hdd_tier):
         
        Policy.__init__(self, cache_size, alpha, ssd_tier, hdd_tier)
        self.p = 0
        self.eviction_queue = deque()
        self.c = cache_size
        self.alpha = alpha
        self.block_size = 1024
        self.ssd_tier = ssd_tier
        self.hdd_tier = hdd_tier
        self.t1 = dict()
        self.t2 = dict()
        self.b1 = dict()
        self.b2 = dict()
        self.output_accumulator = ""
        self.ssd_cache = deque(maxlen=cache_size)
        self.hdd_cache = deque(maxlen=cache_size)
        self.hits_in_hdd_b1_b2 = 0  # Nouvelle métrique pour suivre les hits dans b1 et b2
        # New hit and miss counters
        self.hits = 0
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
    def t1_max_size(self) -> int:
        return self.p

    def t2_max_size(self) -> int:
        return self.c - self.p

    def b1_max_size(self) -> int:
        return self.c - self.p

    def b2_max_size(self) -> int:
        return self.p

    def evict(self):

        file2score = defaultdict(int)

        for i, block in enumerate(self.t1):
            file, offset = block
            file2score[file] += len(self.t1) - i
        for i, block in enumerate(self.t2):
            file, offset = block
            file2score[file] += (len(self.t2) - i) * self.alpha

        file2score = {file: score / (file.size +1) for file, score in file2score.items()}
        if not file2score:
            # No files with scores, so nothing to evict
            return
        #########print('[DEBUG] file_to_score=', file2score)
        #logging.debug('file_to_score=' + str(file2score))
        worse_file = max(file2score, key=file2score.get)
        assert worse_file is not None
        assert worse_file.size > -1
        # Ajout des blocs du fichier à évincer à la file d'attente
        # for offset in range(worse_file.size):
        #     self.eviction_queue.append((worse_file.name, offset))
        # Vérifier si l'un des blocs du fichier est déjà dans self.eviction_queue
        #if not any((worse_file, offset) in self.eviction_queue for offset in range(worse_file.size)):
        #    for offset in range(worse_file.size):
        #        self.eviction_queue.append((worse_file, offset))
        #print("cest eviction_queeu", self.eviction_queue)

        self.remove_all(worse_file)
        # Déplacer le fichier vers le HDD
        self.ssd_tier.remove(worse_file)

        if worse_file in self.ssd_cache:
            self.ssd_cache.remove(worse_file)
        else:
            # Gérer le cas où l'élément n'est pas dans la deque
            print(f"L'élément {worse_file} n'est pas dans la deque.")
        # self.ssd_cache.remove(worse_file)
        self.hdd_cache.appendleft(worse_file)
        self.hdd_tier.add(worse_file)

        # self.evicted_blocks_count += len(self.b1) + len(self.b2)
        self.evicted_blocks_count += worse_file.size
        self.evicted_file_count += 1

        # Calculer la taille des données à transférer en octets
        data_size_to_transfer = (worse_file.size * self.block_size)
        ssd_read_time = data_size_to_transfer / self.ssd_tier.read_throughput
        hdd_write_time = data_size_to_transfer / self.hdd_tier.write_throughput
        max_transfer_time = max(ssd_read_time, hdd_write_time)
        # Calculer le temps de migration
        self.migration_times = max_transfer_time + self.ssd_tier.latency + self.hdd_tier.latency
        print(f'migrate time for this file , {worse_file} , avec la tail {worse_file.size}est  {self.migration_times}')
 
 
 
 
 
 
 
 
    def remove_all(self, file: File):
        """
        Remove all blocks of a file that are in t1 or t2, and add them to b1 and b2, respectively
        """
        
        #logging.debug(f'File {file} marked for unload. State before unload:')
        #logging.debug(self)
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
        
        #logging.debug(f'File {file} marked for unload. State before unload:')
        #logging.debug(self)
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
        for block_offset in range(file.size +1):

            # A block is identified by its file and offset
            block = (file, block_offset)

            # We add the block to the file's block list
            self.file2blocks[file].add(block)

            # We add the block to t1's list
            tier[block] = None

            # If the maximum size of the tier is reached, then we evict
            #if len(self.t1) + len(self.t2) >= self.c:

                # logging.debug(f'Max cache size reached in {"t2" if tier is self.t2 else "t1"} while loading file {file.name}.')
            #    self.evict()
        # check all conditions
        if len(self.t1)+len(self.b1) == self.c:
                if len(self.t1) < self.c:
                    self.b1.popitem()
                    #self.evict()
                   
                else:
                    """self.remove_all(self.t1.popitem()[0][0])
                    self.ssd_tier.remove(self.t1.popitem()[0][0])
                    self.ssd_cache.remove(self.t1.popitem()[0][0])
                    self.hdd_cache.appendleft(self.t1.popitem()[0][0])
                    self.hdd_tier.add( self.t1.popitem()[0][0] )"""
                    self.evict()

        elif len(self.t1) + len(self.t2) + len(self.b1) + len(self.b2) >= self.c:
                if len(self.t1) + len(self.t2) + len(self.b1) + len(self.b2) == self.c * 2:
                    #if self.b2:
                    self.b2.popitem()
                      #self.b2.popitem()
                self.evict()

    def move_file_to(self, file, tier):
        self.remove_all_hard(file)
        self.load_file_to(file, tier)
 

    def on_io(self,file, timestamp,  offset, io_size):
        io_blocks = {(file, offset + i) for i in range(io_size)}
        print(f'offset {timestamp}')
        self.total_time = 0
        self.migration_times = 0
        self.write_times = 0
        self.read_times = 0
        self.prefetch_times = 0
        # Check if all blocks of the I/O are in T1 or T2
        all_blocks_in_cache = all(block in self.t1.keys() or block in self.t2.keys() for block in io_blocks)

        # Count hits and misses for this I/O
        if all_blocks_in_cache:
            self.hits += 1
        else:
            self.misses += 1


        # If no block from this file is in the cache, we want to load all the blocks
        new_file = False
        if not self.file2blocks[file] :
            #########print(f'File {file} is not in cache, loading in t1.')
            #logging.debug(f'File {file} is not in cache, loading in t1.')
            new_file = True

            self.false_misses += 1
            self.load_file_to(file, self.t1)
            self.file2tier[file] = 1

            self.ssd_cache.appendleft(file)
            self.ssd_tier.add(file)

            self.write_times += (file.size * 1024) / self.ssd_tier.write_throughput
            print("write_time caused by this file", self.write_times)
           
        """if file in self.hdd_cache:
            #######print("file in hdd", file)
            # Si le fichier est dans le cache HDD, le déplacer du HDD au SSD
            self.hdd_cache.remove(file)
            self.hdd_tier.remove(file)
            self.ssd_cache.appendleft(file)
            self.ssd_tier.add(file)
            #self.hits_in_hdd_b1_b2 += 1 
            self.prefetch_times += (file.size / self.ssd_tier.read_throughput) + (file.size / self.hdd_tier.write_throughput) + self.ssd_tier.latency + self.hdd_tier.latency
            #print(f" temps de prefetch,{self.prefetch_times}")"""
        # Then, we do the I/O as usual
        for block_offset in range(offset, offset + io_size):

            # A block is identified by its file and offset
            block = (file, block_offset)

            # If a block is in T1 or T2, it's a hit; otherwise, it's a miss
            #if block in self.t1.keys() or block in self.t2.keys():
            #    self.hits += 1
            #else:
            #    self.misses += 1
            # If a block is in T1 and is accessed, we move this single block to T2
            if block in self.t1.keys():
                # if not new_file:
                # if block[1] == 0:
                #     self.read_times += (block[1] * 1024 + 1024) / self.ssd_tier.read_throughput  # lat read
                # else:

                self.read_times += (1024) / self.ssd_tier.read_throughput # lat read
                del self.t1[block]
                self.t2[block] = None

            # If a block is in T2 and is accessed, we move this single block to the top of T2
            elif block in self.t2.keys():
                # if block[1] == 0:
                #     self.read_times += (block[1] * 1024 + 1024) / self.ssd_tier.read_throughput  # lat read
                # else:

                self.read_times += (1024) / self.ssd_tier.read_throughput  # lat read


                #print('cest t2 ', block[1])
                del self.t2[block]
                self.t2[block] = None

            # If an evicted file's block is in B1 and is accessed, we move all its blocks to T2
            elif block in self.b1.keys():
                self.p = min(self.p+(len(self.b2)/len(self.b1), 1)[len(self.b1)>=len(self.b2)], self.c)

                self.move_file_to(file, self.t2)
                #######print( "le fichier a deplacer du hdd a ssd est ", file  )
                self.hdd_cache.remove(file)
                self.hdd_tier.remove(file)
                self.ssd_cache.appendleft(file)
                self.ssd_tier.add(file)
                self.hits_in_hdd_b1_b2 += 1
                # Calculer le temps nécessaire pour lire le fichier depuis le HDD
                hdd_read_time = (file.size * 1024) / self.hdd_tier.read_throughput

                # Calculer le temps nécessaire pour écrire le fichier sur le SSD
                ssd_write_time = (file.size * 1024) / self.ssd_tier.write_throughput

                # Si les opérations de lecture et d'écriture sont en parallèle,
                # prendre le maximum des deux temps
                max_transfer_time = max(hdd_read_time, ssd_write_time)

                # Additionner le temps de transfert maximum et les latences des deux tiers
                total_prefetch_time = max_transfer_time + self.ssd_tier.latency + self.hdd_tier.latency

                # Mettre à jour le temps total de préchargement
                self.prefetch_times = total_prefetch_time
                #self.prefetch_times += ((file.size * 1024) / self.ssd_tier.read_throughput) + ((file.size * 1024)/ self.hdd_tier.write_throughput) + self.ssd_tier.latency + self.hdd_tier.latenc
                ######print(f' hits_on b1_b2: {self.hits_in_hdd_b1_b2}')

                    
            # Same here, but with B2
            elif block in self.b2.keys():
                self.p = max(self.p-(len(self.b1)/len(self.b2), 1)[len(self.b2)>=len(self.b1)], 0)

                self.move_file_to(file, self.t2)
                self.hdd_cache.remove(file)
                self.hdd_tier.remove(file)
                self.ssd_cache.appendleft(file)
                self.ssd_tier.add(file)
                self.hits_in_hdd_b1_b2 += 1
                # Calculer le temps nécessaire pour lire le fichier depuis le HDD
                hdd_read_time = (file.size * 1024) / self.hdd_tier.read_throughput

                # Calculer le temps nécessaire pour écrire le fichier sur le SSD
                ssd_write_time = (file.size * 1024) / self.ssd_tier.write_throughput

                # Si les opérations de lecture et d'écriture sont en parallèle,
                # prendre le maximum des deux temps
                max_transfer_time = max(hdd_read_time, ssd_write_time)

                # Additionner le temps de transfert maximum et les latences des deux tiers
                total_prefetch_time = max_transfer_time + self.ssd_tier.latency + self.hdd_tier.latency

                # Mettre à jour le temps total de préchargement
                self.prefetch_times = total_prefetch_time
                #self.prefetch_times += ((file.size * 1024) / self.ssd_tier.read_throughput) + ((file.size * 1024) / self.hdd_tier.write_throughput) + self.ssd_tier.latency + self.hdd_tier.latency
                # Ajouter le temps de prefetch à la liste
                #self.prefetch_times.append(prefetch_time)
                print(f" temps de prefetch,{self.prefetch_times}")
                ######print(f' hits_on b1_b2: {self.hits_in_hdd_b1_b2}')

                    
            
                    
            #######print('nbr hit v2 %s', self.hits) 
            #######print('nbr miss v2 %s', self.misses) 
        self.reel_misses = self.misses - self.false_misses 
        ######print(f'les varis miss sont : {self.reel_misses}, les faux misses : {self.false_misses}, le total est : {self.misses}')
        ######print(f' hits_on b1_b2: {self.hits_in_hdd_b1_b2}')
        self.total_time = (self.migration_times + self.prefetch_times + self.read_times + self.write_times)
        print(f'total time v2, {self.total_time}')
        print('nbr hit v2 %s', self.hits)
        print('nbr miss v2 %s', self.misses)
        print(len(self.t1) + len(self.t2))
        print(len(self.b1) + len(self.b2))
