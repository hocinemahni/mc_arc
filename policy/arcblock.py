from collections import defaultdict, deque
from  structures.file import File
from .policy import Policy
from structures.deque import Deque
import logging
from .policy import Policy
import datetime
import os 



deques = Deque

    

class Arc_block_Cache(Policy):
    def __init__(self, cache_size, alpha,  ssd_tier, hdd_tier):
         
        Policy.__init__(self, cache_size, alpha= None,  ssd_tier= ssd_tier, hdd_tier= hdd_tier)
        self.cached = {}  # Cache storage
    
        self.c = cache_size   # Cache size
        self.p = 0  # Target size for the list T1
        self.hits = 0  # Counter for cache hits
        self.misses = 0  # Counter for cache misses
        self.evicted_blocks_count = 0  # Counter for evicted blocks
        self.ssd_tier = ssd_tier
        self.hdd_tier = hdd_tier
        self.migration_times = []  # Liste pour stocker les temps de migration
        self.hits_in_hdd_b1_b2 = 0  # Nouvelle métrique pour suivre les hits dans b1 et b2
        self.ssd_cache = deque(maxlen=cache_size)
        self.hdd_cache = deque(maxlen=cache_size)
        self.read_times = 0
        self.write_times = 0
        self.total_time =0
        self.prefetch_times = 0
        # L1: only once recently
        self.t1 = deques()  # T1: recent cache entries
        self.b1 = deques()  # B1: ghost entries recently evicted from the T1 cache

        # L2: at least twice recently
        self.t2 = deques()  # T2: frequent entries
        self.b2 = deques()  # B2: ghost entries recently evicted from the T2 cache

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
            print("old est", old)
            print("old[1]", old[1])
            print("old[1]", old[0])
            self.b1.appendleft(old)
            self.hdd_cache.appendleft(old)
            self.hdd_tier.add( old )
            self.ssd_tier.remove(old)

            self.ssd_cache.remove(old)
            self.evicted_blocks_count += 1
            # Mettre à jour le tuple dans b1 avec l'information complète du fichier
           
            
        else:
           
            
            old = self.t2.pop()
            
            self.b2.appendleft(old)
            self.ssd_tier.remove(old)
            self.ssd_cache.remove(old)
            self.hdd_cache.appendleft(old)
            self.hdd_tier.add( old )
            
            self.evicted_blocks_count += 1
            
            # Calculer la taille des données à transférer en octets (exemple)
            data_to_transfer = old
            data_size_to_transfer = (old[1] - old[0]) * 4096   
            # Calculer le temps de migration
            migration_time = (data_size_to_transfer / self.ssd_tier.read_throughput) + (data_size_to_transfer / self.hdd_tier.write_throughput) + self.ssd_tier.latency + self.hdd_tier.latency
            print(f'migrate time for this file , {data_to_transfer} , avec la tail {data_size_to_transfer} est  {migration_time}')
            
            #print("old[1]", old[1])
            #print("old[1]", old[0])
            # Ajouter le temps de migration à la liste
            
            self.migration_times.append(migration_time) 
            # Mettre à jour le tuple dans b2 avec l'information complète du fichier
        
        del self.cached[old]
        print(f'Tier {self.ssd_tier.name} is nearly full, migrating files'
                
               
                f' \n content : {(self.ssd_cache)}'
               
                f' \n  ssd_tier : {(self.ssd_tier)}'
                f' \n  hdd_tier : {(self.hdd_tier)}'
                f' \n used size : {(self.hits_in_hdd_b1_b2)}'
                f' \n used size : {len(self.t1)+ len(self.t2)}'
                )  
    def on_io(self, file: File, offset: int, io_size: int):
        # Simulate I/O operation and update cache
        result = "Data from offset {} to io_size {}".format(offset, offset + io_size)
       
        args = (offset, offset + io_size, file)
        if args in self.t1:
            self.t1.remove(args)
            self.t2.appendleft(args)
            self.hits += 1
            return self.cached[args]

        if args in self.t2:
          
            self.t2.remove(args)
            self.t2.appendleft(args)
            self.hits += 1
            return self.cached[args]
        #logging.info(self)
        self.misses += 1
        result = "Data from offset {} to io_size {}".format(offset, offset + io_size)
        self.cached[args] = result

        if args in self.b1:
            
            self.p = min(self.c, self.p + max(len(self.b2) / len(self.b1), 1))
            self.replace(args)
            self.b1.remove(args)
            self.t2.appendleft(args)
            self.hdd_cache.remove(args)
            self.hdd_tier.remove(args)
            self.ssd_cache.appendleft(args)
            self.ssd_tier.add( args)
            # prefetch +1
            self.hits_in_hdd_b1_b2 += 1
            return result

        if args in self.b2:
           
            self.p = max(0, self.p - max(len(self.b1) / len(self.b2), 1))
            self.replace(args)
            self.b2.remove(args)
            self.t2.appendleft(args)
            self.hdd_cache.remove(args)
            self.hdd_tier.remove(args)
            self.ssd_cache.appendleft(args)
            self.ssd_tier.add( args)

            # prefetch +1
            self.hits_in_hdd_b1_b2 += 1
            return result

        if len(self.t1) + len(self.b1) == self.c:
            if len(self.t1) < self.c:
               
                self.b1.pop()
                #logging.info('élément {} supprimé directement du cache'.format(self.b1.pop()))
                self.replace(args)
            else:
                #logging.info(self)
                
                del self.cached[self.t1.pop()]
                self.ssd_tier.remove(self.t1.pop())
                if (self.t1.pop()) in self.ssd_cache:
                   self.ssd_cache.remove(self.t1.pop())
                else:
                # Gérer le cas où l'élément n'est pas dans la deque
                     print(f"L'élément {self.t1.pop()} n'est pas dans la deque.")
                #self.ssd_cache.remove(self.t1.pop())
                self.hdd_cache.appendleft(self.t1.pop())
                self.hdd_tier.add( self.t1.pop() )
                 
                self.evicted_blocks_count += 1
                #logging.info('élément {} supprimé directement du cache'.format(self.t1.pop()))
                
                # Calculer la taille des données à transférer en octets (exemple)
                data_to_transfer = self.t1.pop()
                data_size_to_transfer = ((data_to_transfer[1] - data_to_transfer[0]) * 4096)  # offsetend - offsetdeb
                
                # Calculer le temps de migration
                migration_time = (data_size_to_transfer / self.ssd_tier.read_throughput) + (data_size_to_transfer / self.hdd_tier.write_throughput) + self.ssd_tier.latency + self.hdd_tier.latency
                print(f'migrate time for this file , {data_to_transfer} , avec la tail {data_size_to_transfer} est  {migration_time}')
                # Ajouter le temps de migration à la liste
                print("self.t1.pop()) est", data_to_transfer)
                print("self.t1.pop())[1]", data_to_transfer[1])
                print("self.t1.pop())[0]", data_to_transfer[0])
                self.migration_times.append(migration_time)  

        else:
            total = len(self.t1) + len(self.b1) + len(self.t2) + len(self.b2)
            if total >= self.c:
               
                if total == (2 * self.c):
                    self.b2.pop()
                self.replace(args)
        
        self.t1.appendleft(args)
        #logging.debug('C\'est le,nombre de migration: %s', (self.evicted_blocks_count))
        #logging.debug('C\'est le,nombre de block dans le cache: %s', (self.cached))
        #logging.debug('C\'est le,nombre de block dans le cache: %s', (self.t1) + (self.t2))
        """print(f'Tier {self.ssd_tier.name} is nearly full, migrating files'
                
               
                f' \n content : {(self.ssd_cache)}'
               
                f' \n  ssd_tier : {(self.ssd_tier)}'
                f' \n  hdd_tier : {(self.hdd_tier)}'
                f' \n used size : {(self.hits_in_hdd_b1_b2)}'
                f' \n used size : {len(self.t1)+ len(self.t2)}'
                )"""
        
        #logging.debug('C\'est le nombre total de blocks dans le cache : %s', int((len(self.t1)+len(self.t2))))
        return result


    """"def __repr__(self) -> str:
        output = "\n".join(("",
            f't1|empty|t2 (▓.▒) [{"▓"*int(len(self.t1)*50/self.c)}' \
            f'{"."*int(50-(len(self.t1)+len(self.t2))*50/self.c)}' \
            f'{"▒"*int(len(self.t2)*50/self.c)}] (len(t1)={len(self.t1)}'\
            f', len(t2)={len(self.t2)}, total={self.c}, unused={self.c-len(self.t1)-len(self.t2)})',"",
            f'b1|empty|b2 (▓.▒) [{"▓"*int(len(self.b1)*50/self.c)}' \
            f'{"."*int(50-(len(self.b1)+len(self.b2))*50/self.c)}' \
            f'{"▒"*int(len(self.b2)*50/self.c)}] (len(b1)={len(self.b1)}'\
            f', len(b2)={len(self.b2)}, total={self.c}, unused={self.c-len(self.b1)-len(self.b2)})'))
     

        #t1_content = [f'({file}, {block_offset2})' for file, block_offset2, file in self.t1]
        #t2_content = [f'({file}, {block_offset2})' for file, block_offset2, file in self.t2]
        #b1_content = [f'({file}, {block_offset2})' for file, block_offset2, file in self.b1]
        #b2_content = [f'({file}, {block_offset2})' for file, block_offset2, file in self.b2]
        t1_content = [f'({file}, {block_offset}, {block_offset2})' for file, block_offset, block_offset2 in self.t1]
        t2_content = [f'({file}, {block_offset}, {block_offset2})' for file, block_offset, block_offset2 in self.t2]
        b1_content = [f'({file}, {block_offset}, {block_offset2})' for file, block_offset, block_offset2 in self.b1]
        b2_content = [f'({file}, {block_offset}, {block_offset2})' for file, block_offset, block_offset2 in self.b2]

    
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
        log_filename = os.path.join(logs_dir, f'arcblk_{current_datetime}.txt')
        logging.basicConfig(filename=log_filename, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M')
        # Écrire le contenu dans un fichier externe
        try:
            with open(log_filename, 'a', encoding='utf-8') as f:
                f.write(output + '\n')
        except Exception as e:
            logging.error(f"An error occurred while writing the output to a file: {e}") 
            
        
        # Enregistrement du contenu dans le journal
        logging.info(output)
        #logging.debug('C\'est le temps de migration v2: %s', sum(self.migration_times))
        #logging.debug('cest le temps de migration v2', sum(self.migration_times) )
        logging.debug('le temps de migration %s',sum(self.migration_times) )
        logging.debug('C\'elements t1: %s', len(self.t1))
        logging.debug('C\'est elements t2: %s', len(self.t2))
        logging.debug('C\'elements b1: %s', len(self.b1))
        logging.debug('C\'elements b2 %s', len(self.b2))
        logging.debug('cest le temps de migration arcblock %s', sum(self.migration_times) )
        return output   """

