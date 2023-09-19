from structures.tier import Tier 
import os
import matplotlib.pyplot as plt
import pandas as pd
import datetime
import matplotlib
import numpy as np  # Importez numpy pour vérifier NaN
import time
from multiprocessing import Process, Manager
from structures.file import File
from policy.arcfile_v11 import ARC_File_Policyv11
import pstats
from policy.arcfile_v2 import ARC_File_Policyv2
from policy.arcblock import Arc_block_Cache
#from policy.arcfilev1blck import ARC_File_Policyv1b
#from policy.arvfilev2blck import ARC_File_Policyv2b
#from policy.arv2bockfile import ARC_File_Policyv2bb
#from policy.arcblockblock import Arc_block_Cacheb
import logging
###logging.getLogger('matplotlib.font_manager').setLevel(logging.ERROR)
import math



# Configurer le journal
current_datetime = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
##logs_dir = 'logs'
#os.makedirs(logs_dir, exist_ok=True)
#log_filename = os.path.join(logs_dir, f'simulation_{current_datetime}.txt')
#logging.basicConfig(filename=log_filename, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
#block_size = 64000
block_size = 4096
def simulate_policy(policy, ios, cache_size, alpha, policy_name, hits_data, misses_data, evicted_blocks_data, times_migration, total_times ):
    policy_hits_data = []
    policy_misses_data = []
    policy_evicted_blocks_data = []
    policy_tieme_migration = []
    policy_total_times = []
    s = 0
    for file, offset, io_size in ios: 
        #start_time = time.time()
       if np.isnan(offset) or block_size == 0:
        offset_block = 0  # Valeur par défaut en cas de NaN ou block_size égal à zéro
       else: 
        offset_block = math.floor(offset / block_size)
        size_block = math.ceil((offset + io_size) / block_size) - offset_block
        
        #file = next(file for file in files if file.name == filename)
        policy.on_io(file, offset_block, size_block)
        policy_hits_data.append(policy.hits)
        policy_misses_data.append(policy.misses)
        policy_evicted_blocks_data.append(policy.evicted_blocks_count)
        policy_tieme_migration.append(policy.migration_times)
        policy_total_times.append(policy.total_time)
        #####logging.info(f"{policy_name} - Hits and Misses: {policy.hits} {policy.misses}")
        ###logging.info(f"{policy_name} - Number of evicted blocks: {policy.evicted_blocks_count}")
        ###logging.info(f"{policy_name} - Number of evicted filess: {policy.evicted_file_count}")
        #logging.info(f"migration times for v2: {sum(arc_file_policy_v2.migration_times)}")
        #print(f"{policy_name} - Number of evicted filess: {policy.evicted_file_count}")
        s += 1
        #print(f"{policy_name} - Number of io : {s}")
    total_times.append(policy_total_times) 
    hits_data.append(policy_hits_data)
    misses_data.append(policy_misses_data)
    #if policy_name == "ARC_File_Policyv1":
    #    evicted_blocks_data[0].extend(policy_evicted_blocks_data)
    #elif policy_name == "ARC_File_Policyv2":
       # evicted_blocks_data[1].extend(policy_evicted_blocks_data)
    evicted_blocks_data.append(policy_evicted_blocks_data)
    times_migration.append(policy.migration_times)
    
def simulate_arc_block_policy(ios, cache_size, arc_block_hits_data, arc_block_misses_data, arc_block_evicted_blocks_data, arc_migration_time, arc_total_times):
    arc_block_policy = Arc_block_Cache(cache_size,alpha, ssd_tier, hdd_tier)
    #arc_block_misses_data = []
    #arc_block_evicted_blocks_data = []
    #arc_block_hits_data =[]
    s =0
    for file, offset, io_size in ios:
       if np.isnan(offset) or block_size == 0:
        offset_block = 0  # Valeur par défaut en cas de NaN ou block_size égal à zéro
       else:  
        offset_block = math.floor(offset / block_size)
        size_block = math.ceil((offset + io_size) / block_size) - offset_block
        
        #file = next(file for file in files if file.name == filename)
        #policy.on_io(file, offset, io_size)
        arc_block_policy.on_io(file, offset_block, size_block)
        arc_block_hits_data.append(arc_block_policy.hits)
        arc_block_misses_data.append(arc_block_policy.misses)
        arc_block_evicted_blocks_data.append(arc_block_policy.evicted_blocks_count)
        arc_migration_time.append(arc_block_policy.migration_times)
        arc_total_times.append(arc_block_policy.total_time)
        ###loging.info(f"Arc_block_Cache - Hits and Misses: {arc_block_policy.hits} {arc_block_policy.misses}")
        ###loging.info(f"Arc_block_Cache - Number of evicted blocks: {arc_block_policy.evicted_blocks_count}")
        ###loging.info(f"Arc_block_Cache - Migration time: {arc_block_policy.migration_times}")
        s += 1
        ##print('io to arcblk %s', s)
    return arc_block_hits_data, arc_block_misses_data, arc_block_evicted_blocks_data, arc_migration_time, arc_total_times
if __name__ == "__main__":
 
  ####cache_size = round(1679721600//4096)
  cache_size = round(201326592 / 4096)
  #cache_size = round(1677721600/ 64000) # 40 files (1,34217728 go) 33,554432 mb = 33554432 octet
  alpha_values = [1.00]
  alpha = 1.0  # Initialisez alpha avec une valeur


  ssd_tier = Tier("SSD", max_size=cache_size, latency=0.00001, read_throughput=1000000000, write_throughput=1000000000)
  hdd_tier = Tier("HDD", max_size=cache_size, latency=0.005, read_throughput=200000000, write_throughput=150000000)

  # Charger les données des fichiers à partir de "data/filess2.csv"
  files_data = pd.read_csv("data/metadataibms.csv")
  files = [File(row['filename'], round(row['size'] / 4096)) for _, row in files_data.iterrows()]
  #files = [File(row['filename'], row['size'] ) for _, row in files_data.iterrows()]
  current_datetime = time.strftime("%Y%m%d_%H%M%S")
   # Charger les données d'E/S à partir de "data/ioss.csv"
  ios_data = pd.read_csv("data/iosibms.csv")
  ios = [(file, row['offset'], row['io_size']) for _, row in ios_data.iterrows() for file in files if file.name == row['filename']]
  
  ##print(ios)

  ios_count = list(range(1, len(ios) + 1))
  for alpha in alpha_values:
    # Créer des instances de chaque politique
    arc_file_policy_v1 = ARC_File_Policyv11(cache_size, alpha, ssd_tier, hdd_tier)
    arc_file_policy_v2 = ARC_File_Policyv2(cache_size, alpha, ssd_tier, hdd_tier)
   
    # Créer des listes partagées pour stocker les données
    #manager = Manager()
    #migration_times = manager.list()
   
    #hits_data = manager.list()
    #misses_data = manager.list()
    #evicted_blocks_data = manager.list([[], []])  # Initialize with empty lists for v1 and v2
    #evicted_blocks_data = manager.list()
    #arc_block_hits_data = manager.list()
    #times_migration = manager.list()
    #arc_migration_time = manager.list()
    #arc_block_misses_data = manager.list()
    #arc_block_evicted_blocks_data = manager.list()
    # Créer des processus séparés pour simuler chaque politique
    # Définir les listes pour stocker les données
    hits_data = []
    misses_data = []
    evicted_blocks_data = []
    migration_time = [] 
    total_times = []
    arc_block_hits_data = []
    arc_block_misses_data = [] 
    arc_block_evicted_blocks_data = []
     # Créez des listes pour stocker les données de temps de migration pour chaque politique
   
    arc_migration_time = []
    arc_total_times = []
    # Simuler chaque politique de manière séquentielle
    for policy, policy_name in [(arc_file_policy_v1, "ARC_File_Policyv1"),
                            (arc_file_policy_v2, "ARC_File_Policyv2")]:
        simulate_policy(policy, ios, cache_size,alpha, policy_name, hits_data, misses_data, evicted_blocks_data, migration_time, total_times)
       
         
    arc_block_hits_data, arc_block_misses_data, arc_block_evicted_blocks_data, arc_migration_time, arc_total_times  = simulate_arc_block_policy(ios, cache_size, arc_block_hits_data[:], arc_block_misses_data[:], arc_block_evicted_blocks_data[:], arc_migration_time[:], arc_total_times[:])
    # Ajoutez les données de temps de migration pour Arc_block_Cache
    
    
    #arc_block_process.join()
         
  
    # Tracer les données pour chaque politique dans deux figures
    plt.figure(figsize=(18, 12))

    # Figure 1: Misses et Hits
    #plt.figure(1)

    # Tracer pour ARC_File_Policyv1
    plt.subplot(3, 1, 1)
    plt.plot(ios_count, hits_data[0], label='Hits (ARC_File_Policyv1)')  # Modifier la légende ici
    plt.plot(ios_count, misses_data[0], label='Misses (ARC_File_Policyv1)')  # Modifier la légende ici)
    plt.xlabel('Number of I/Os')
    plt.ylabel('Count')
    plt.title('ARC_File_Policyv1 Hits and Misses')
    plt.legend()
    # Afficher les nombres de hits et misses pour ARC_File_Policyv1
    if hits_data and misses_data:
      plt.text(0.75, 0.9, f"Hits: {hits_data[0][-1]}\nMisses: {misses_data[0][-1]}", transform=plt.gca().transAxes, fontsize=12, verticalalignment='top')
      plt.text(0.5, 0.9, f"alpha: {alpha}", transform=plt.gca().transAxes, fontsize=12, verticalalignment='top'  )
    # Tracer pour ARC_File_Policyv2
    plt.subplot(3, 1, 2)
    
    plt.plot(ios_count, hits_data[1], label='Hits (ARC_File_Policyv2)')
    plt.plot(ios_count, misses_data[1], label='Misses (ARC_File_Policyv2)')
    plt.xlabel('Number of I/Os')
    plt.ylabel('Count')
    plt.title('ARC_File_Policyv2 Hits and Misses')
    plt.legend()
    # Afficher les nombres de hits et misses pour ARC_File_Policyv2
    plt.text(0.75, 0.9, f"Hits: {hits_data[1][-1]}\nMisses: {misses_data[1][-1]}", transform=plt.gca().transAxes, fontsize=12, verticalalignment='top')
    plt.text(0.5, 0.9, f"alpha: {alpha}", transform=plt.gca().transAxes, fontsize=12, verticalalignment='top'  )
    # Tracer pour Arc_block_Cache
    plt.subplot(3, 1, 3)
    plt.plot(ios_count, arc_block_hits_data, label='Hits (Arc_block_Cache)')
    plt.plot(ios_count, arc_block_misses_data, label='Misses (Arc_block_Cache)')
    plt.xlabel('Number of I/Os')
    plt.ylabel('Count')
    plt.title('Arc_block_Cache Hits and Misses')
    plt.legend()
    # Afficher les nombres de hits et misses pour Arc_block_Cache
    plt.text(0.75, 0.9, f"Hits: {arc_block_hits_data[-1]}\nMisses: {arc_block_misses_data[-1]}", transform=plt.gca().transAxes, fontsize=12, verticalalignment='top')

    plt.tight_layout()
    #plt.show()
    # Sauvegarder les figures dans le dossier "figures"
    if not os.path.exists('figures'):
        os.makedirs('figures')
    #current_datetime = time.strftime("%Y%m%d_%H%M%S")
    figure_filename1 = os.path.join('figures', f'figures_{current_datetime}.png')
    plt.savefig(figure_filename1)
    plt.show()
    plt.close()

    # Figure 2: Blocs évincés
    #plt.figure(2)
    plt.figure(figsize=(18, 6))
    # Tracer pour ARC_File_Policyv1
    plt.subplot(3, 1, 1)
    plt.plot(ios_count, evicted_blocks_data[0], label='Evicted Blocks')
    plt.text(0.75, 0.2, f"Evictions (ARC_File_Policyv1): {evicted_blocks_data[0][-1]}", transform=plt.gca().transAxes, fontsize=16, verticalalignment='top')
    plt.xlabel('Number of I/Os')
    plt.ylabel('Count')
    plt.title('ARC_File_Policyv1 Evicted Blocks')
    plt.legend()

    # Tracer pour ARC_File_Policyv2
    plt.subplot(3, 1, 2)
    plt.plot(ios_count, evicted_blocks_data[1], label='Evicted Blocks')
    plt.text(0.75, 0.2, f"Evictions (ARC_File_Policyv2): {evicted_blocks_data[1][-1]}", transform=plt.gca().transAxes, fontsize=16, verticalalignment='top')
    plt.xlabel('Number of I/Os')
    plt.ylabel('Count')
    plt.title('ARC_File_Policyv2 Evicted Blocks')
    plt.legend()

    # Tracer pour Arc_block_Cache
    plt.subplot(3, 1, 3)
    plt.plot(ios_count, arc_block_evicted_blocks_data, label='Evicted Blocks')
     # Afficher le nombre d'évictions pour Arc_block_Cache
    plt.text(0.75, 0.2, f"Evictions (Arc_block_Cacheb): {arc_block_evicted_blocks_data[-1]}", transform=plt.gca().transAxes, fontsize=16, verticalalignment='top')
    plt.xlabel('Number of I/Os')
    plt.ylabel('Count')
    plt.title('Arc_block_Cache Evicted Blocks')
    plt.legend()
    

    
    
    



    #plt.tight_layout(rect=[0, 0, 1, 0.95]) 
    plt.tight_layout()
    #plt.show()
    # Sauvegarder les figures dans le dossier "figures"
    figure_filename2 = os.path.join('figures', f'evicted_blocks_{current_datetime}.png')
    plt.savefig(figure_filename2)
    plt.show()
    plt.close()
    #plt.figure(figsize=(10, 6))
    
    # Figure 3: Temps de migration
    #plt.figure(3)

   # Créer un graphe comparatif des temps de migration
    plt.figure(figsize=(12, 8))
    """#migration_times_list = [sum(times_migration[0]), sum(times_migration[1])] 
    #migration_times_list.extend(arc_migration_time)
    migration_times_list = [sum(policy_times) for policy_times in migration_time]
    arc_migration_time_flat = [time for sublist in arc_migration_time for time in sublist]
    migration_times_list.append(sum(arc_migration_time_flat))
    """
    total_times_list = [sum(policy_times) for policy_times in total_times]
    #arc_total_times_flat = [time for sublist in arc_total_times for time in sublist]
    arc_total_times_flat = []

    for sublist in arc_total_times:
      if isinstance(sublist, list):
        arc_total_times_flat.extend(sublist)
      else:
        arc_total_times_flat.append(sublist)

    
    total_times_list.append(sum(arc_total_times_flat))

    policy_names = ["ARC_File_Policyv2", "ARC_File_Policyv1", "Arc_block_Cache" ]
    plt.bar(policy_names, total_times_list)
    plt.xlabel("Policy", fontsize=14)
    plt.ylabel("Total  Time (seconds)", fontsize=14)
    plt.title("Comparison of Total Times between Policies", fontsize=14)
    # Add text labels with the migration times on top of the bars
    for i, time in enumerate(total_times_list):
      plt.text(i, time + 1, f"{time:.2f} s", color='black', fontsize=16)
    plt.xticks(fontsize=12)  # Ajustez la taille de la police des étiquettes de l'axe x
    plt.yticks(fontsize=12)  # Ajustez la taille de la police des étiquettes de l'axe y


    # Sauvegarder la figure dans le dossier "figures"
    #current_datetime = time.strftime("%Y%m%d_%H%M%S")
    figure_filename = os.path.join('figures', f'total_times_list{current_datetime}.png')
    plt.savefig(figure_filename)
    
    plt.tight_layout()
     
    # Afficher 
    plt.show()
    plt.close()
  
    
    
