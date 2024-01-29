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
        else:
            self.evict()
            self.cache[file] = None

    def on_io(self, file, timestamp, requestType, offsetStart, offsetEnd):
  

        if file not in self.cache:
            # Si le fichier n'est pas dans le cache            
            misses = offsetEnd - offsetStart
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
            hits = offsetEnd - offsetStart
            # Marquer le fichier comme récemment utilisé
            self.cache.move_to_end(file)


        return hits, misses


