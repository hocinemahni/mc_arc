# -*- coding: utf-8 -*-
from collections import defaultdict, deque, OrderedDict
from structures.file import File
from policy.policy import Policy
import datetime


# LRU policy
class LRU(Policy):
    def __init__(self, cache_size,users, alpha, ssd_tier, hdd_tier, log_file_path, total_io_count):
        Policy.__init__(self, cache_size, alpha, ssd_tier, hdd_tier)
        self.user_requests = defaultdict(int)  # Nombre de requêtes par utilisateur
        self.user_request_sizes = defaultdict(int)  # Taille totale des requêtes par utilisateur
        self.user_total_time = defaultdict(float)  # Pour stocker le temps total par utilisateur
        self.p = 0
        self.io_counter = 0
        self.total_io_operations = total_io_count
        self.users = users
        self.mis_block = 0
        self.hit_block = 0
        self.c = cache_size
        self.alpha = alpha
        self.block_size = 1024
        self.ssd_tier = ssd_tier
        self.hdd_tier = hdd_tier
        self.log_file_path = log_file_path
        self.lru_list = OrderedDict()
        self.output_accumulator = ""
        self.ssd_cache = deque(maxlen=cache_size)
        self.hdd_cache = deque(maxlen=cache_size)
        self.hits_in_hdd_b1_b2 = 0
        # New hit and miss counters
        self.hits = 0
        self.misses = 0
        self.false_misses = 0
        self.user_total_time = defaultdict(float)
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
        self.ssd_time = 0
        self.hdd_time = 0
        self.hdd_time_pref = 0
        self.ssd_time_evict = 0
        self.hdd_time_evict = 0
        self.nbr_of_blocks_hdd_reads = 0
        self.files_to_evict_immediately = deque()

    def evict(self):
        worse_file_tuple, _ = self.lru_list.popitem(last=False)
        print(f'le fichier {worse_file_tuple[0].name} est identifié pour l\'éviction')
        worse_file = worse_file_tuple[0]  # Extraire l'objet File du tuple
        print(
            f'le fichier {worse_file} est identifié pour l\'éviction avec le nom {worse_file.name} et la taille {worse_file.size}')

        # nombre de fichier dans ssd et hdd

        # Déplacer le fichier vers le HDD
        self.ssd_tier.remove_file(worse_file.name)
        self.hdd_tier.add_file(worse_file)
        self.users[worse_file.user_id].decrease_space(worse_file.size)
        self.evicted_blocks_count += worse_file.size
        self.evicted_file_count += 1
        self.remove_all(worse_file)

        # Calculer la taille des données à transférer en octets
        data_size_to_transfer = (worse_file.size * self.block_size)
        ssd_read_time = (data_size_to_transfer / self.ssd_tier.read_throughput) + self.ssd_tier.latency
        hdd_write_time = (data_size_to_transfer / self.hdd_tier.write_throughput) + self.hdd_tier.latency
        self.ssd_time += ssd_read_time
        self.hdd_time += hdd_write_time

    def remove_all(self, file: File):
        """
        Remove all blocks of a file that are in LRU list
        """
        # logging.debug(f'File {file} marked for unload. State before unload:')
        # logging.debug(self)
        blocks_LRU = [block for block in self.lru_list if block[0] == file]

        for block in blocks_LRU:
            del self.lru_list[block]
        del self.file2blocks[file]
        self.file2tier[file] = 0

    def remove_all_hard(self, file: File):
        """
        Remove all blocks of a file from LRU
        """
        blocks = self.file2blocks[file]
        for block in blocks:

            del self.lru_list[block]
        del self.file2blocks[file]
        self.file2tier[file] = 0

    def load_file_to(self, file, tier):
        """ Charger un fichier dans un niveau de stockage.
        Args:
            file (File): Le fichier à charger.
            tier (int): Le niveau de stockage dans lequel charger le fichier.
        """

        if file.size <= (self.c - (len(self.lru_list))):
            for block_offset in range(file.size):
                # A block is identified by its file and offset
                block = (file, block_offset)

                # We add the block to the file's block list
                self.file2blocks[file].add(block)

                # We add the block to LRU list
                tier[block] = None
            self.ssd_tier.add_file(file)
            self.file2tier[file] = 1
            self.users[file.user_id].increase_space(file.size)
        else:
            while file.size > (self.c - (len(self.lru_list))):
                self.evict()

    def move_file_to(self, file, tier):
        """ Déplacer un fichier d'un niveau de stockage à un autre.
        """
        self.remove_all_hard(file)
        self.load_file_to(file, tier)

    def calculate_user_throughput(self):
        """Calculer et retourner le débit pour chaque utilisateur."""
        user_throughputs = {}
        for user_id in self.users:
            if self.user_total_time[user_id] > 0:  # Éviter la division par zéro
                throughput = (self.user_requests[user_id] / self.user_total_time[user_id]) * self.user_request_sizes[
                    user_id]
                user_throughputs[user_id] = throughput
        return user_throughputs

    def log_user_times(self):
        """Les temps d'utilisation de chaque utilisateur dans un fichier log, y compris le débit."""
        # Calculer les débits pour tous les utilisateurs avant d'écrire dans le fichier
        user_throughputs = self.calculate_user_throughput()

        with open(self.log_file_path, 'a') as log_file:
            log_file.write(f"Time updated at: {datetime.datetime.now()}\n")
            for user_id in self.users:
                time_spent = self.users[user_id].time_spent
                space_default = self.users[user_id].space_default
                space_used = self.users[user_id].space_utilization
                throughput = user_throughputs.get(user_id, 0)  # Obtention du débit pour cet utilisateur, 0 par défaut si non calculé
                log_file.write(f"User {user_id}: Throughput = {throughput:.2f} bytes/sec | Total Time = {time_spent} seconds | Default Space = {space_default} | Used Space = {space_used}\n")
            log_file.write("\n")
    def on_io(self, file, timestamp, requestType, offsetStart, offsetEnd):
        """Traiter une requête d'entrée/sortie.
        file: Fichier sur lequel la requête est effectuée
        timestamp: Temps de la requête
        requestType: Type de requête (lecture, écriture, prélecture)
        offsetStart: Début de la requête
        offsetEnd: Fin de la requête
        """
        #print('file', file, 'timestamp', timestamp, 'requestType', requestType, 'offsetStart', offsetStart, 'offsetEnd', offsetEnd)
        self.ssd_time = 0
        self.time_now = timestamp
        self.hdd_time = 0
        self.ssd_time_evict = 0
        self.hdd_time_evict = 0
        self.hdd_time_pref = 0
        self.total_time = 0
        user_id = file.user_id
        request_size = offsetEnd - offsetStart  # Taille de la requête actuelle

        # Mise à jour des données de débit
        self.user_requests[user_id] += 1
        self.user_request_sizes[user_id] += request_size * self.block_size  # Convertir en octets
        self.write_times = self.read_times = self.prefetch_times = self.migration_times = 0
        self.isinb2 = False
        self.hdd_time = 0
        new_file = False
        self.io_counter += 1
        for block_offset in range(offsetStart, offsetEnd):
            block = (file, block_offset)
            if block in self.lru_list and new_file is False:
                self.hits += 1
            else:
                # Si le bloc n'est pas dans LRU, c'est un 'miss'
                self.misses += 1
            # Vérifier si le bloc est dans LRU
            if block in self.lru_list:
                if not new_file:
                    self.ssd_time += (self.block_size / self.ssd_tier.read_throughput) + self.ssd_tier.latency
                    self.lru_list.move_to_end(block, last=True)
                else:
                    self.ssd_time += (self.block_size / self.ssd_tier.read_throughput) + self.ssd_tier.latency
            elif self.hdd_tier.is_file_in_tier(file.name) and not self.ssd_tier.is_file_in_tier(file.name):
                if file.size <= self.c:
                    new_file = True
                    self.hdd_tier.remove_file(file.name)
                    self.false_misses += 1
                    self.remove_all_hard(file)
                    self.load_file_to(file, self.lru_list)

                    # Calculer le temps nécessaire pour lire le fichier depuis le HDD
                    hdd_read_time = ((file.size * self.block_size) / self.hdd_tier.read_throughput) + \
                                    self.hdd_tier.latency

                    # Calculer le temps nécessaire pour écrire le fichier sur le SSD
                    ssd_write_time = ((file.size * self.block_size) / self.ssd_tier.write_throughput) + \
                                     self.ssd_tier.latency

                    self.ssd_time += ssd_write_time
                    self.hdd_time += hdd_read_time
                # Si le fichier est trop grand pour le SSD, le lire directement depuis le HDD
                if file.size > self.c:
                    self.hdd_time += ( self.block_size / self.hdd_tier.read_throughput) + \
                                     self.hdd_tier.latency
                    self.nbr_of_blocks_hdd_reads += 1
            else:
                if not self.hdd_tier.is_file_in_tier(file.name) and not self.ssd_tier.is_file_in_tier(file.name) :
                    # Si le fichier n'est pas dans le SSD ou le HDD, le charger dans le SSD
                    if file.size <= self.c:
                        # Si le fichier est plus petit que la taille du SSD, le charger dans le SSD
                        new_file = True
                        self.false_misses += 1
                        self.load_file_to(file, self.lru_list)
                        self.ssd_time += ((file.size * self.block_size) / self.ssd_tier.write_throughput) + \
                                         self.ssd_tier.latency
                    else:
                        # Si le fichier est trop grand pour le SSD, le charger dans le HDD
                        self.hdd_tier.add_file(file)
                        self.hdd_time += ((file.size * self.block_size) / self.hdd_tier.read_throughput) + \
                                         self.hdd_tier.latency
                        self.nbr_of_blocks_hdd_reads += 1

        self.total_time += self.ssd_time + self.hdd_time
        self.users[file.user_id].increase_time_spent(self.total_time)
        self.user_total_time[file.user_id] += self.total_time
        if self.io_counter == self.total_io_operations:
            print('cest le dernier fichier')
            self.log_user_times()
            self.io_counter = 0  # Réinitialiser le compteur d'opérations d'entrée/sortie

