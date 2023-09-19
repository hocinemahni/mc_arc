from collections import defaultdict, deque
from structures.file import File
from .policy import Policy
import os
import datetime

import logging

class ARC_File_Policyv11(Policy):
    def __init__(self, cache_size, alpha,  ssd_tier, hdd_tier):
         
        Policy.__init__(self, cache_size, alpha, ssd_tier, hdd_tier)
        self.p = 0
        self.c = cache_size
        self.alpha = alpha
        
        self.t1 = dict()
        self.t2 = dict()
        self.b1 = dict()
        self.b2 = dict()
        self.ssd_tier = ssd_tier
        self.hdd_tier = hdd_tier
        self.ssd_cache = deque(maxlen=cache_size)
        self.hdd_cache = deque(maxlen=cache_size)
        self.hits_in_hdd_b1_b2 = 0  # Nouvelle métrique pour suivre les hits dans b1 et b2
        
        # New hit and miss counters
        self.hits = 0
        self.misses = 0
        self.false_misses = 0
        # number of blocks évicted
        self.evicted_blocks_count = 0
        self.evicted_file_count = 0
        self.prefetch_times = 0
        self.evicted_blocks_data = []  #  list to store evicted blocks data
        self.read_times = 0
        self.write_times = 0
        self.total_time =0
        self.file2tier = defaultdict(int)
        self.file2blocks = defaultdict(set)
        self.migration_times = 0  # une liste vide pour stocker les temps de migration

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

        # On calcule le score de chaque fichier en fonction de sa présence dans T1 et T2
        for file in set([file for file, _ in self.t1.keys()] + [file for file, _ in self.t2.keys()]):
            for i, block in enumerate(self.t1):
                if block[0] == file:
                    file2score[file] += len(self.t1) - i
            for i, block in enumerate(self.t2):
                if block[0] == file:
                    file2score[file] += (len(self.t2) - i) * self.alpha

        file2score = {file: score / file.size for file, score in file2score.items()}
        if not file2score:
          # No files with scores, so nothing to evict
          return
        ##print('[DEBUG] file_to_score=', file2score)
        ###logging.debug('file_to_score=' + str(file2score))

        worse_file = max(file2score, key=file2score.get)
        assert worse_file is not None
        assert worse_file.size > 0
        
        
        self.remove_all(worse_file)
        #self.evicted_blocks_count += len(self.b1) + len(self.b2)
        #print(worse_file.size)
        
         # Déplacer le fichier vers le HDD
        self.hdd_cache.appendleft(worse_file)
        self.hdd_tier.add( worse_file )
        self.ssd_tier.remove(worse_file)
        if worse_file in self.ssd_cache:
          self.ssd_cache.remove(worse_file)
        else:
           # Gérer le cas où l'élément n'est pas dans la deque
            print(f"L'élément {worse_file} n'est pas dans la deque.")
        
        self.evicted_blocks_count += worse_file.size
        self.evicted_file_count += 1

        # Calculer la taille des données à transférer en octets 
        data_size_to_transfer = (worse_file.size * 4096)  
        # Calculer le temps de migration
        
        self.migration_times += (data_size_to_transfer / self.ssd_tier.read_throughput) + (data_size_to_transfer / self.hdd_tier.write_throughput) + self.ssd_tier.latency + self.hdd_tier.latency
        print(f'migrate time for this file , {worse_file} , avec la tail {worse_file.size}est  {self.migration_times}')
        # Ajouter le temps de migration à la liste
        #self.migration_times.append(migration_time)
        print(f'Tier {self.ssd_tier.name} is nearly full, migrating files'
                
               
                f' \n content : {(self.ssd_cache)}'
               
                f' \n  ssd_tier : {(self.ssd_tier)}'
                f' \n  hdd_tier : {(self.hdd_tier)}'
                f' \n used size : {(self.hits_in_hdd_b1_b2)}'
                f' \n used size : {len(self.t1)+ len(self.t2)}'
                )  

        
        
                ##print('cest v1 (self.migration_times)')
            
    def remove_all(self, file: File):
        """
        Remove all blocks of a file that are in t1 or t2, and add them to b1 and b2, respectively
        """
        
        ###logging.debug(f'File {file} marked for unload. State before unload:')
        ###logging.debug(self)
        blocks_t1 = [block for block in self.t1 if block[0] == file]
        blocks_t2 = [block for block in self.t2 if block[0] == file]
        
        for block in blocks_t1:
            del self.t1[block]
            self.b1[block] = None

        for block in blocks_t2:
            del self.t2[block]
            self.b2[block] = None 


        # On enlève également le fichier de la liste des fichiers en cache
        self.file2tier[file] = 0
        del self.file2blocks[file]

    def remove_all_hard(self, file: File):
        """
        Remove all blocks of a file from t1, t2, b1 or b2
        """
        
        ###loging.debug(f'File {file} marked for unload. State before unload:')
        ######loging.debug(self)
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
        # On enlève également le fichier de la liste des fichiers en cache
        self.file2tier[file] = 0
        del self.file2blocks[file]

    def load_file_to(self, file, tier):
        for block_offset in range(file.size):
            # A block is identified by its file and offset
            block = (file, block_offset)

            # We add the block to the file's block list
            self.file2blocks[file].add(block)

            # We add the block to t1's list
            tier[block] = None

        # If the maximum size of the tier is reached, then we evict
            if len(self.t1) + len(self.t2) >= self.c:
              
              ##########loging.debug(f'Max cache size reached in {"t2" if tier is self.t2 else "t1"} while loading file {file.name}.')
              self.evict()

    def move_file_to(self, file, tier):
        
        self.remove_all_hard(file)
        self.load_file_to(file, tier)


    

    def on_io(self, timestamp, file: File, offset: int, io_size: int):
        io_blocks = {(file, offset + i) for i in range(io_size)}
        print(f'offset {timestamp}') 
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
            ##########loging.debug(f'File {file} is not in cache, loading in t1.')
            new_file = True
            self.load_file_to(file, self.t1)
            self.file2tier[file] = 1
            self.false_misses += 1
            self.ssd_cache.appendleft(file)
            self.ssd_tier.add(file)
            
            self.write_times += (file.size * 4096) / self.hdd_tier.write_throughput 
            # Ajouter le temps de migration à la liste
            #self.write_times.append(write_time)

           
          
        if file in self.hdd_cache:
            # Si le fichier est dans le cache HDD, le déplacer du HDD au SSD
            print('file in hdd', file)
            self.hdd_cache.remove(file)
            self.hdd_tier.remove(file)
            self.ssd_cache.appendleft(file)
            self.ssd_tier.add( file)
            #self.hits_in_hdd_b1_b2 += 1 
            self.prefetch_times += (file.size / self.ssd_tier.read_throughput) + (file.size / self.hdd_tier.write_throughput) + self.ssd_tier.latency + self.hdd_tier.latency
            # Ajouter le temps de prefetch à la liste
            #self.prefetch_times.append(self.prefetch_times)
            print(f" temps de prefetch,{self.prefetch_times}")
            
        # Then, we do the I/O as usual
        for block_offset in range(offset, offset + io_size):

            # A block is identified by its file and offset
            block = (file, block_offset)
            
            # If a block is in T1 and is accessed, we move the whole file to T2
            if block in self.t1.keys():
                if not new_file:
                    self.read_times += (block[1]* 4096) / self.hdd_tier.write_throughput # lat read 
                    self.move_file_to(file, self.t2)
                    break

            # If a block is in T2 and is accessed, we move this single block to the top of T2
            elif block in self.t2.keys():
                if not new_file:
                    self.read_times += (block[1]* 4096) / self.hdd_tier.write_throughput 
                    self.move_file_to(file, self.t2)
                    break
               
            # If an evicted file's block is in B1 and is accessed, we move all its blocks to T2
            elif block in self.b1.keys():
                self.p = min(self.p+(len(self.b2)/len(self.b1), 1)[len(self.b1)>=len(self.b2)], self.c)
                if file.size < self.c - (len(self.t1) + len(self.t2)):
                 self.move_file_to(file, self.t2)
                 print( "le fichier a deplacer du hdd a ssd est ", file  )
                 self.hdd_cache.remove(file)
                 self.hdd_tier.remove(file)
                 self.ssd_cache.appendleft(file)
                 self.ssd_tier.add( file)
                 self.hits_in_hdd_b1_b2 += 1
                 self.prefetch_times += ((file.size * 4096) / self.ssd_tier.read_throughput) + ((file.size * 4096)/ self.hdd_tier.write_throughput) + self.ssd_tier.latency + self.hdd_tier.latency
                 # Ajouter le temps de prefetch à la liste
                 #self.prefetch_times.append(prefetch_time)
                 print(f" temps de prefetch,{self.prefetch_times}")
                else:
                    self.evict()
            # Same here, but with B2
            elif block in self.b2.keys():
                self.p = max(self.p-(len(self.b1)/len(self.b2), 1)[len(self.b2)>=len(self.b1)], 0)
                if file.size < self.c - (len(self.t1) + len(self.t2)):
                 self.move_file_to(file, self.t2)
                 self.prefetch_times += ((file.size * 4096) / self.ssd_tier.read_throughput) + ((file.size * 4096) / self.hdd_tier.write_throughput) + self.ssd_tier.latency + self.hdd_tier.latency
                 # Ajouter le temps de prefetch à la liste
                 #self.prefetch_times.append(prefetch_time)
                 print(f" temps de prefetch,{self.prefetch_times}")

                 self.hdd_cache.remove(file)
                 self.hdd_tier.remove(file)
                 self.ssd_cache.appendleft(file)
                 self.ssd_tier.add(file)
                 self.hits_in_hdd_b1_b2 += 1 
                else:
                    self.evict()    

            if len(self.t1) + len(self.b1) == self.c:
              if len(self.t1) < self.c:
                self.b1.popitem()
                self.evict()
              else:
                """self.remove_all(self.t1.popitem()[0][0])
                self.ssd_tier.remove(self.t1.popitem()[0][0])
                self.ssd_cache.remove(self.t1.popitem()[0][0])
                self.hdd_cache.appendleft(self.t1.popitem()[0][0])
                self.hdd_tier.add( self.t1.popitem()[0][0] )"""
                self.evict()
            elif len(self.t1) + len(self.t2) + len(self.b1) + len(self.b2) >= self.c:
              if len(self.t1) + len(self.t2) + len(self.b1) + len(self.b2) == self.c * 2:
                self.b2.popitem()
                self.evict()
        
        
        self.total_time += (self.migration_times + self.prefetch_times + self.read_times + self.write_times)
        print(f'total time, {self.total_time}')
        self.reel_misses = self.misses - self.false_misses 
        print(f'les varis miss sont : {self.reel_misses}, les faux misses : {self.false_misses}, le total est : {self.misses}')
        
        #print('nbr hit v1 %s', self.hits) 
        #print('nbr misses v1 %s', self.misses)          
    """def __repr__(self) -> str:
        output = "\n".join(("",
            f't1|empty|t2 (▓.▒) [{"▓"*int(len(self.t1) *50/self.c )}' \
            f'{"."*int(50-(len(self.t1)+len(self.t2))*50/self.c)}' \
            f'{"▒"*int(len(self.t2)*50/self.c)}] (len(t1)={len(self.t1)}'\
            f', len(t2)={len(self.t2)}, total={self.c}, unused={self.c-len(self.t1)-len(self.t2)})',"",
            f'b1|empty|b2 (▓.▒) [{"▓"*int(len(self.b1)*50/self.c)}' \
            f'{"."*int(50-(len(self.b1)+len(self.b2))*50/self.c)}' \
            f'{"▒"*int(len(self.b2)*50/self.c)}] (len(b1)={len(self.b1)}'\
            f', len(b2)={len(self.b2)}, total={self.c}, unused={self.c-len(self.b1)-len(self.b2)})'))
        t1_content = [f'{file.name}: [{", ".join(sorted([str(block_offset2) for file2, block_offset2 in self.t1.keys() if file2 == file and block_offset2 is not None]))}]'
                      for file in set([file for file, block_offset in self.t1.keys()])]
        t2_content = [f'{file.name}: [{", ".join(sorted([str(block_offset2) for file2, block_offset2 in self.t2.keys() if file2 == file and block_offset2 is not None]))}]'
                      for file in set([file for file, block_offset in self.t2.keys()])]
        b1_content = [f'{file.name}: [{", ".join(sorted([str(block_offset2) for file2, block_offset2 in self.b1.keys() if file2 == file and block_offset2 is not None]))}]'
                      for file in set([file for file, block_offset in self.b1.keys()])]
        b2_content = [f'{file.name}: [{", ".join(sorted([str(block_offset2) for file2, block_offset2 in self.b2.keys() if file2 == file and block_offset2 is not None]))}]'
                      for file in set([file for file, block_offset in self.b2.keys()])]
        output += "\n".join((["\n\nt1 is empty", "\n\nt1 contains:\n  - "+"\n  - ".join(t1_content)][len(self.t1)>0],
                           ["t2 is empty", "t2 contains:\n  - "+"\n  - ".join(t2_content)][len(self.t2)>0],
                           ["b1 is empty", "b1 contains:\n  - "+"\n  - ".join(b1_content)][len(self.b1)>0],
                           ["b2 is empty", "b2 contains:\n  - "+"\n  - ".join(b2_content)][len(self.b2)>0], "#"*65))
        # Obtenir la date actuelle au format YYYY-MM-DD_HH-MM-SS
        current_datetime = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M')

        # Créer un dossier pour les journaux s'il n'existe pas
        logs_dir = 'logs'
        os.makedirs(logs_dir, exist_ok=True)

        # Configurer la journalisation avec la date dans le nom du fichier
        log_filename = os.path.join(logs_dir, f'arcv1_{current_datetime}.txt')
        logging.basicConfig(filename=log_filename, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M')
        # Écrire le contenu dans un fichier externe
        try:
            with open(log_filename, 'a', encoding='utf-8') as f:
                f.write(output + '\n')
        except Exception as e:
            logging.error(f"An error occurred while writing the output to a file: {e}") 
            
        
        # Enregistrement du contenu dans le journal
        logging.info(output)
        
        #logging.debug('C\'est le temps de migration v1: %s', sum(self.migration_times))

        #logging.debug('cest le temps de migration v1',sum(self.migration_times) )
        logging.debug('[DEBUG] migration times=%s', sum(self.migration_times))
        return output"""
    
                
        
