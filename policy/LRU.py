from collections import OrderedDict
from structures.file import File
from policy.policy import Policy

class LRU(Policy):
    def __init__(self, cache_size, ssd_tier, hdd_tier):
        super().__init__(cache_size, ssd_tier, hdd_tier)
        self.cache = OrderedDict()
        self.cache_size = cache_size
        self.block_size = 1024
        self.ssd_tier = ssd_tier
        self.hdd_tier = hdd_tier
        self.read_times = 0
        self.write_times = 0
        self.total_time = 0
        self.prefetch_times = 0
        self.migration_times_evict = 0
        self.total_eviction_time = 0
        self.evicted_blocks_count = 0
        self.evicted_file_count = 0
        self.file2blocks = defaultdict(set)
        self.file2tier = defaultdict(int)
        self.hits = 0
        self.misses = 0

    def evict(self):
        # Évince des fichiers jusqu'à ce qu'il y ait assez d'espace pour le nouveau fichier
        while self.cache_size < self.total_size_in_cache():
            _, file_to_evict = self.cache.popitem(last=False)
            self.remove_all(file_to_evict)
            self.evicted_blocks_count += file_to_evict[0].size
            # Calculer la taille des données à transférer en octets
            data_size_to_transfer = (file_to_evict[0].size * self.block_size)
            # Calculer le temps de migration
            ssd_read_time = (data_size_to_transfer / self.ssd_tier.read_throughput) + self.ssd_tier.latency
            hdd_read_time = (data_size_to_transfer / self.hdd_tier.write_throughput) + self.hdd_tier.latency
            self.migration_times += max(ssd_read_time, hdd_read_time)

    def remove_all(self, file: File):
        # Supprime un fichier et tous ses blocs du cache
        if file in self.cache:
            del self.cache[file]

    def total_size_in_cache(self):
        # Calcule la taille totale des fichiers dans le cache
        return sum(file.size for file in self.cache)

    def load_file_to_cache(self, file):
        # Charge un fichier dans le cache, en évinçant d'autres fichiers si nécessaire
        if file.size + self.total_size_in_cache() <= self.cache_size:
            self.cache[file] = None
            self.ssd_tier.add_file(file)
        else:
            self.evict()
            self.cache[file] = None
            self.ssd_tier.add_file(file)

    def on_io(self, file, timestamp, requestType, offsetStart, offsetEnd):
  

        if file not in self.cache:
            # Si le fichier n'est pas dans le cache            
            self.misses = offsetEnd - offsetStart
            if self.hdd_tier.is_file_in_tier(file.name):
               self.hdd_tier.remove_file(file.name)
               self.ssd_tier.add_file(file)
               self.load_file_to_cache(file)
               self.hits_in_hdd_b1_b2 += 1
               # Calculer le temps nécessaire pour lire le fichier depuis le HDD
               hdd_read_time = ((file.size * self.block_size) / self.hdd_tier.read_throughput) + self.hdd_tier.latency
               # Calculer le temps nécessaire pour écrire le fichier sur le SSD
               ssd_write_time = ((file.size * self.block_size) / self.ssd_tier.write_throughput) + self.ssd_tier.latency               
               # prendre le maximum des deux temps
               self.prefetch_times += max(hdd_read_time, ssd_write_time)
            elif file.size < self.cache_size:
               self.load_file_to_cache(file)
               self.ssd_tier.add_file(file)
               self.ssd_time += ((file.size * self.block_size) / self.ssd_tier.write_throughput) + self.ssd_tier.latency
            else: 
                self.hdd_tier.add_file(file)
                self.hdd_time += ((file.size * self.block_size) / self.hdd_tier.write_throughput) + self.hdd_tier.latency             
        else:
            # Si le fichier est déjà dans le cache, tous les blocs demandés sont des hits
            self.hits = offsetEnd - offsetStart
            self.ssd_time += ((( offsetEnd - offsetStart)* self.block_size) / self.ssd_tier.read_throughput) + self.ssd_tier.latency
            # Marquer le fichier comme récemment utilisé
            self.cache.move_to_end(file)
            
        self.total_time += self.ssd_time + self.hdd_time + self.migration_times + self.prefetch_times
        print('nbr hit', self.hits)
        print('nbr miss', self.misses)
        


