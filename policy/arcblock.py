from collections import defaultdict, deque
from structures.file import File
from .policy import Policy
from structures.deque import Deque
from .policy import Policy


deques = Deque


class Arc_block_Cache(Policy):
    def __init__(self, cache_size, alpha, ssd_tier, hdd_tier):

        Policy.__init__(self, cache_size, alpha, ssd_tier, hdd_tier)
        #self.cached = {}  # Cache storage
        self.hit_block = 0
        self.mis_block = 0
        self.block_size = 1024
        self.c = cache_size  # Cache size
        self.p = 0  # Target size for the list T1
        self.hits = 0  # Counter for cache hits
        self.misses = 0  # Counter for cache misses
        self.evicted_blocks_count = 0  # Counter for evicted blocks
        self.ssd_tier = ssd_tier
        self.hdd_tier = hdd_tier
        self.evicted_file_count = 0
        self.migration_times = 0  # Liste pour stocker les temps de migration
        self.hits_in_hdd_b1_b2 = 0
        self.read_times = 0
        self.write_times = 0
        self.total_time = 0
        self.prefetch_times = 0
        # L1: only once recently
        self.t1 = deques()  # T1: recent cache entries
        self.b1 = deques()  # B1: ghost entries recently evicted from the T1 cache
        # L2: at least twice recently
        self.t2 = deques()  # T2: frequent entries
        self.b2 = deques()  # B2: ghost entries recently evicted from the T2 cache
        self.ssd_time = 0
        self.hdd_time = 0
        self.hdd_time_pref = 0
        self.ssd_time_evict = 0
        self.hdd_time_evict = 0
    def replace(self, args):
        """
        If (T1 is not empty) and ((T1 length exceeds the target p) or (x is in B2 and T1 length == p))
            Delete the LRU page in T1 (also remove it from the cache), and move it to MRU position in B1.
        else
            Delete the LRU page in T2 (also remove it from the cache), and move it to MRU position in B2.
        endif
        """

        if self.t1 and ((args in self.b2 and len(self.t1) == self.p) or (len(self.t1) > self.p)):
            old = self.t1.pop()
            self.b1.appendleft(old)
            self.hdd_tier.add_block(*old)
            self.ssd_tier.remove_block(*old)
            # Calculer la taille des données à transférer en octets (exemple)
            data_size_to_transfer = self.block_size
            # Calculer le temps de migration
            # Calculer le temps nécessaire pour lire le fichier depuis le HDD
            #ssd_read_time = (data_size_to_transfer / self.ssd_tier.read_throughput)
            #self.ssd_time_evict += (data_size_to_transfer / self.ssd_tier.read_throughput) #+ self.ssd_tier.latency
            #self.hdd_time_evict += (data_size_to_transfer / self.hdd_tier.write_throughput) #+ self.hdd_tier.latency
            # Calculer le temps nécessaire pour écrire le fichier sur le SSD
            #hdd_write_time = (data_size_to_transfer / self.hdd_tier.write_throughput)

            # Si les opérations de lecture et d'écriture sont en parallèle,
            # prendre le maximum des deux temps
            #max_transfer_time = max(ssd_read_time, hdd_write_time)
            # Calculer le temps nécessaire pour lire le fichier depuis le HDD
            ssd_read_time = (data_size_to_transfer / self.ssd_tier.read_throughput)
            # Calculer le temps nécessaire pour écrire le fichier sur le SSD
            hdd_write_time = (data_size_to_transfer / self.hdd_tier.write_throughput)
            # self.ssd_time_evict += (data_size_to_transfer / self.ssd_tier.read_throughput) + self.ssd_tier.latency
            # self.hdd_time_evict += (data_size_to_transfer / self.hdd_tier.write_throughput) + self.hdd_tier.latency
            # Si les opérations de lecture et d'écriture sont en parallèle,
            # prendre le maximum des deux temps
            max_transfer_time = max(ssd_read_time, hdd_write_time)

            # Additionner le temps de transfert maximum et les latences des deux tiers
            # self.migration_times += (max_transfer_time + self.ssd_tier.latency + self.hdd_tier.latency)
            self.migration_times += max_transfer_time
            # Additionner le temps de transfert maximum et les latences des deux tiers
            #self.migration_times += (max_transfer_time + self.ssd_tier.latency + self.hdd_tier.latency)
            self.evicted_blocks_count += 1
            # Mettre à jour le tuple dans b1 avec l'information complète du fichier
        else:
            old = self.t2.pop()
            self.b2.appendleft(old)
            self.ssd_tier.remove_block(*old)
            self.hdd_tier.add_block(*old)
            self.evicted_blocks_count += 1
            # Calculer la taille des données à transférer en octets (exemple)
            data_size_to_transfer = self.block_size
            # Calculer le temps de migration
            # Calculer le temps nécessaire pour lire le fichier depuis le HDD
            ssd_read_time = (data_size_to_transfer / self.ssd_tier.read_throughput)
            # Calculer le temps nécessaire pour écrire le fichier sur le SSD
            hdd_write_time = (data_size_to_transfer / self.hdd_tier.write_throughput)
            #self.ssd_time_evict += (data_size_to_transfer / self.ssd_tier.read_throughput) + self.ssd_tier.latency
            #self.hdd_time_evict += (data_size_to_transfer / self.hdd_tier.write_throughput) + self.hdd_tier.latency
            # Si les opérations de lecture et d'écriture sont en parallèle,
            # prendre le maximum des deux temps
            max_transfer_time = max(ssd_read_time, hdd_write_time)

            # Additionner le temps de transfert maximum et les latences des deux tiers
            #self.migration_times += (max_transfer_time + self.ssd_tier.latency + self.hdd_tier.latency)
            self.migration_times += max_transfer_time
    def on_io(self, file, timestamp, requestType, offsetStart, offsetEnd):
        # Simulate I/O operation and update cache
        self.total_time = 0
        # Initialisation des variables pour vérifier les hits
        self.ssd_time = 0
        self.hdd_time = 0
        self.ssd_time_evict = 0
        self.hdd_time_evict = 0
        self.hdd_time_pref = 0

        for block_offset in range(offsetStart, offsetEnd):
            args = (file, block_offset)
            #
            if args in self.t1 or args in self.t2:
                #hits = True
                self.hits += 1
            else:
                # Si le bloc n'est pas dans t1 ou t2, c'est un 'miss'
                #hits = False
                self.misses += 1

            if args in self.t1:
                self.t1.remove(args)
                self.t2.appendleft(args)
                self.ssd_time += (self.block_size / self.ssd_tier.read_throughput)
            elif args in self.t2:
                self.t2.remove(args)
                self.t2.appendleft(args)
                self.ssd_time += (self.block_size / self.ssd_tier.read_throughput)
            elif args in self.b1:
                self.p = min(self.c, self.p + max(round(len(self.b2) / len(self.b1)), 1))
                self.replace(args)
                self.b1.remove(args)
                self.t2.appendleft(args)
                self.ssd_tier.add_block(*args)
                #self.hdd_time_pref += (self.block_size / self.hdd_tier.read_throughput) #+ self.hdd_tier.latency
                self.hdd_tier.remove_block(*args)
                hdd_read_time = (self.block_size / self.hdd_tier.read_throughput)
                ssd_write_time = (self.block_size / self.ssd_tier.write_throughput)
                # Calculer le temps nécessaire pour écrire le fichier sur le SSD

                # Si les opérations de lecture et d'écriture sont en parallèle,
                # prendre le maximum des deux temps
                max_transfer_time = max(hdd_read_time, ssd_write_time)

                # Additionner le temps de transfert maximum et les latences des deux tiers
                total_prefetch_time = max_transfer_time

                # Mettre à jour le temps total de préchargement
                self.prefetch_times += total_prefetch_time
                self.hits_in_hdd_b1_b2 += 1

            elif args in self.b2:
                self.p = max(0, self.p - max(round(len(self.b1) / len(self.b2)), 1))
                self.replace(args)
                self.b2.remove(args)
                self.t2.appendleft(args)
                self.ssd_tier.add_block(*args)
                self.hdd_time_pref += (self.block_size / self.hdd_tier.read_throughput) #+ self.hdd_tier.latency
                self.hdd_tier.remove_block(*args)
                hdd_read_time = (self.block_size / self.hdd_tier.read_throughput)
                # Calculer le temps nécessaire pour écrire le fichier sur le SSD
                ssd_write_time = (self.block_size / self.ssd_tier.write_throughput)
                # Si les opérations de lecture et d'écriture sont en parallèle,
                # prendre le maximum des deux temps
                max_transfer_time = max(hdd_read_time, ssd_write_time)

                # Additionner le temps de transfert maximum et les latences des deux tiers
                #total_prefetch_time = (max_transfer_time + self.ssd_tier.latency + self.hdd_tier.latency)
                total_prefetch_time = max_transfer_time
                # Mettre à jour le temps total de préchargement
                self.prefetch_times += total_prefetch_time
                self.hits_in_hdd_b1_b2 += 1

            elif args not in self.t1 and args not in self.b1 and args not in self.t2 and args not in self.b2:
                if len(self.t1) + len(self.b1) == self.c:
                    if len(self.t1) < self.c:
                        self.b1.pop()
                        self.replace(args)
                    else:
                        if len(self.t1) == 0:
                            # No block with scores, so nothing to evict
                            return
                        else:
                            t1pop = self.t1.pop()
                            self.ssd_tier.remove_block(*t1pop)
                            self.evicted_blocks_count += 1
                            data_size_to_transfer = self.block_size

                            # Calculer le temps nécessaire pour lire le fichier depuis le HDD
                            ssd_read_time = data_size_to_transfer / self.ssd_tier.read_throughput

                            # Calculer le temps nécessaire pour écrire le fichier sur le SSD
                            hdd_write_time = data_size_to_transfer / self.hdd_tier.write_throughput

                            # Si les opérations de lecture et d'écriture sont en parallèle,
                            # prendre le maximum des deux temps
                            max_transfer_time = max(ssd_read_time, hdd_write_time)

                            # Additionner le temps de transfert maximum et les latences des deux tiers
                            total_eviction_time = max_transfer_time

                            # Mettre à jour le temps total de préchargement
                            self.migration_times += total_eviction_time
                else:
                    total = len(self.t1) + len(self.b1) + len(self.t2) + len(self.b2)
                    if total >= self.c:

                        if total == (2 * self.c):
                            self.b2.pop()
                        self.replace(args)

                self.t1.appendleft(args)
                if self.hdd_tier.is_block_in_file(*args):
                    self.hdd_tier.remove_block(*args)
                    self.ssd_tier.add_block(*args)
                    self.hdd_time += (self.block_size / self.hdd_tier.read_throughput) #+ self.hdd_tier.latency
                    hdd_read_time = (self.block_size / self.hdd_tier.read_throughput)
                    ssd_write_time = (self.block_size / self.ssd_tier.write_throughput)
                    # Calculer le temps nécessaire pour écrire le fichier sur le SSD

                    # Si les opérations de lecture et d'écriture sont en parallèle,
                    # prendre le maximum des deux temps
                    max_transfer_time = max(hdd_read_time, ssd_write_time)

                    # Additionner le temps de transfert maximum et les latences des deux tiers
                    total_prefetch_time = (max_transfer_time + self.ssd_tier.latency + self.hdd_tier.latency)

                    # Mettre à jour le temps total de préchargement
                    self.prefetch_times += total_prefetch_time
                else:
                    if self.c < 1:

                        self.hdd_tier.add_block(*args)
                        #self.write_times += ((self.block_size) / self.hdd_tier.write_throughput)
                        self.hdd_time += (self.block_size / self.hdd_tier.read_throughput) + self.hdd_tier.latency
                    else:
                        self.ssd_tier.add_block(*args)
                        self.ssd_time += (self.block_size / self.ssd_tier.write_throughput) + self.ssd_tier.latency
        self.total_time = self.ssd_time + self.hdd_time + self.prefetch_times + self.migration_times
        print('nbr hit arc block %s', self.hits)
        print('nbr miss arc block %s', self.misses)
        #print('nombre de blocks evincés ', self.evicted_blocks_count)
        print('taille du cache  %s', self.c)
        print('la taille de t1 et t2 :', len(self.t1) + len(self.t2))
        print('la taille de b1 et b2 :', len(self.b1) + len(self.b2))
