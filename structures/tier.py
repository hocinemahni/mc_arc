from typing import List

""""class Tier:
    def __init__(self,
                 name: str,
                 max_size: int,
                 latency: float,
                 throughput: float,
                 target_occupation: float = 1.0):
        self.name = name
        self.max_size = max_size
        self.used_size = 0
        self.latency = latency
        self.throughput = throughput
        self.target_occupation = target_occupation
        
        self.content = dict()  # key: File, value: List[blocks(int)]
        
        self.number_of_ios = 0
        self.number_of_eviction_from_this_tier = 0
        self.number_of_eviction_to_this_tier = 0
        self.number_of_prefetching_from_this_tier = 0
        self.number_of_prefetching_to_this_tier = 0

        self.total_io_time = 0
        self.user_io_time = 0 # io time without considering the time spent doing migrations (eviction/prefetch)
    
    def list_content():
        pass

    def add_content():
        pass

    def migrate_content():
        pass"""

class Tier:
    def __init__(self, name, max_size, latency, read_throughput, write_throughput ):

        """ :param name: name of the tier
        :param max_size: octets
        :param latency: nanoseconds
        :param read_throughput: o/nanoseconds
        :param write_throughput: o/nanoseconds"""
        self.name = name
        self.max_size = max_size
        self.latency = latency
        self.read_throughput = read_throughput
        self.write_throughput = write_throughput
        self.files = {}
        self.size = 0 

    def add(self, file):
        # Simuler l'ajout d'un fichier au tier
        if file not in self.files:
            if isinstance(file, tuple):
             
             #  ajout de  la taille par bloc
             self.size += (file[1] - file[0] ) #  la taille du bloc soit le deuxième élément du tuple
             
            else: 
              self.files[file] = file
              self.size += file.size

    """ def remove(self, file_name):
        # Simuler la suppression d'un fichier du tier
        if file_name in self.files:
            removed_file = self.files[file_name]
            del removed_file
            self.size -= removed_file.size"""

    def remove(self, file):
        # Simuler la suppression d'un fichier du tier
        if file in self.files:
            removed_file = self.files.pop(file)
            self.size -= removed_file.size  # Soustraire la taille du fichier supprimé
            print('remove file size', removed_file.size)
        else:
            print(f"File '{file}' not found in the tier '{self.name}'")
    def __str__(self):
        return f"Tier {self.name}: Size={self.size}/{self.max_size}, Latency={self.latency}, Throughput={self.write_throughput} , et Throughput={self.read_throughput} "
