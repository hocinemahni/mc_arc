from collections import OrderedDict
from structures.file import File
from policy.policy import Policy

class LRU(Policy):
    def __init__(self, cache_size, ssd_tier, hdd_tier):
        super().__init__(cache_size, ssd_tier, hdd_tier)
        self.cache = OrderedDict()

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
        hits, misses = 0, 0

        if file not in self.cache:
            # Si le fichier n'est pas dans le cache
            self.load_file_to_cache(file)
            misses = offsetEnd - offsetStart
        else:
            # Si le fichier est déjà dans le cache, tous les blocs demandés sont des hits
            hits = offsetEnd - offsetStart
            # Marquer le fichier comme récemment utilisé
            self.cache.move_to_end(file)


        return hits, misses


