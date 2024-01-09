
class Tier:

    def __init__(self, name, max_size, latency, read_throughput, write_throughput):

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
        self.block_size = 1024

    def add_block(self, file_name, block):
        # Vérifier si le fichier existe déjà
        if file_name not in self.files:
            self.files[file_name] = set()

        # Ajouter le bloc au fichier, en respectant la limite de taille
        if self.size + self.block_size <= self.max_size:
            self.files[file_name].add(block)
            self.size += self.block_size

    def remove_block(self, file_name, block):
        # Vérifier si le fichier existe
        if file_name in self.files:
            # Vérifier si le bloc existe dans le fichier
            if block in self.files[file_name]:
                self.files[file_name].remove(block)
                self.size -= self.block_size
            # else:
            #     print("Bloc non trouvé dans le fichier.")
        # else:
        #     print("Fichier non trouvé.")

    def is_block_in_file(self, file_name, block):
        # Vérifier si le fichier existe
        if file_name in self.files:
            # Retourner True si le bloc est trouvé dans le fichier
            return block in self.files[file_name]
        else:
            # Retourner False si le fichier n'existe pas
            return False

    def add_file(self, file):
        if self.size + file.size <= self.max_size:
            self.files[file.name] = file
            self.size += file.size
        # else:
        #     print("Capacité maximale atteinte, impossible d'ajouter le fichier.")

    def remove_file(self, file_name):
        if file_name in self.files:
            self.size -= self.files[file_name].size
            del self.files[file_name]
        # else:
        #     print("Fichier non trouvé.")

    def is_file_in_tier(self, file_name):
        return file_name in self.files

    def __str__(self):
        return f"Tier {self.name}: Size={self.size}/{self.max_size}, Latency={self.latency}, Throughput={self.write_throughput} , et Throughput={self.read_throughput} "
