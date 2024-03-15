import datetime
from structures.io import IORequest
from collections import deque
from structures.tier import Tier
from structures.file import File
from policy.Idle_Time_BFH_ARC import Idle_Time_BFH_ARC
from policy.arcblock import Arc_block_Cache
from policy.FG_ARC import FG_ARC
# from policy.arcv1 import ARC_File_Policyv1
from policy.BFH_ARC import BFH_ARC
from policy.RLT_ARC import RLT_ARC
from policy.lru import LRU
from policy.arcfilewithlifetime import ARC_File_Policyv2lifetime
from policy.BFH_ARC_whith_alpha_beta import BFH_ARC_alpha_beta
from policy.BFH_ARC_whithout_alpha_beta import BFH_ARC_whithout_alpha_beta
from policy.cfs import CFS_ARC
import math
import datetime
import matplotlib.pyplot as plt
import numpy as np
import os

# Fonction pour traiter une demande d'IO
def process_io_request_with_queue(io_request, previous_end_time, policy, policy_hits_data, policy_misses_data,
                                  policy_evicted_fils_data,
                                  policy_evicted_blocks_data, policy_time_migration, policy_total_times,
                                  policy_prefetch_times, policy_read_times, policy_write_times):
    """ Traite une demande IO
    :param io_request: Demande IO à traiter
    :param previous_end_time: Temps de fin de la dernière demande IO
    :param policy: Politique de remplacement à utiliser
    :param policy_hits_data: Liste pour stocker les hits de la politique
    :param policy_misses_data: Liste pour stocker les misses de la politique
    :param policy_evicted_fils_data: Liste pour stocker les fichiers évacués par la politique
    :param policy_evicted_blocks_data: Liste pour stocker les blocs évacués par la politique
    :param policy_time_migration: Liste pour stocker les temps de migration de la politique
    :param policy_total_times: Liste pour stocker les temps totaux de la politique
    :param policy_prefetch_times: Liste pour stocker les temps de prefetch de la politique
    :param policy_read_times: Liste pour stocker les temps de lecture de la politique
    :param policy_write_times: Liste pour stocker les temps d'écriture de la politique
    """
    # Calculer le temps de début d'exécution de la demande IO
    io_request.execution_start_time = max(io_request.timestamp, previous_end_time)

    # Calculer le nombre de blocs à lire/écrire
    offset_block = math.floor(io_request.offsetStart / block_size)
    end_block = math.ceil(io_request.offsetEnd / block_size)
    # size_block = end_block - offset_block

    # Exécuter la politique de remplacement
    policy.on_io(io_request.file, io_request.timestamp, io_request.requestType, offset_block, end_block)

    # Mettre à jour les listes de données
    policy_hits_data.append(policy.hits)
    policy_misses_data.append(policy.misses)
    policy_evicted_fils_data.append(policy.evicted_file_count)
    policy_evicted_blocks_data.append(policy.evicted_blocks_count)
    #policy_time_migration.append(policy.migration_times)
    policy_total_times.append(policy.total_time)
    #policy_prefetch_times.append(policy.prefetch_times)
    #policy_read_times.append(policy.read_times)
    #policy_write_times.append(policy.write_times)
    #io_request.execution_end_time = io_request.execution_start_time + policy.total_time
    io_request.execution_end_time = io_request.execution_start_time + policy.ssd_time
    return io_request.execution_end_time

# Fonction pour traiter une demande d'IO
def process_io_request_with_queue1(io_request, previous_end_time, policy, policy_hits_data, policy_misses_data,
                                   policy_evicted_fils_data,
                                   policy_evicted_blocks_data, policy_time_migration, policy_total_times,
                                   policy_prefetch_times, policy_read_times, policy_write_times):
    """ Traite une demande IO
    Args:
        io_request (IORequest): La demande IO à traiter
        previous_end_time (float): Le temps de fin de la dernière demande IO
        policy (Policy): La politique de remplacement à utiliser
        policy_hits_data (list): La liste pour stocker les hits de la politique
        policy_misses_data (list): La liste pour stocker les misses de la politique
        policy_evicted_fils_data (list): La liste pour stocker les fichiers évictés de la politique
        policy_evicted_blocks_data (list): La liste pour stocker les blocs évictés de la politique
        policy_time_migration (list): La liste pour stocker les temps de migration de la politique
        policy_total_times (list): La liste pour stocker les temps totaux de la politique
        policy_prefetch_times (list): La liste pour stocker les temps de prefetch de la politique
        policy_read_times (list): La liste pour stocker les temps de lecture de la politique
        policy_write_times (list): La liste pour stocker les temps d'écriture de la politique
    Returns:
        float: Le temps de fin de la demande IO traitée et la demande IO traitée
    """

    #  La demande IO est traitée après la fin de la dernière demande IO
    io_request.execution_start_time = max(io_request.timestamp, previous_end_time)

    #  Calculer les blocs à partir de l'offset et de la taille de la demande IO
    offset_block = math.floor(io_request.offsetStart / block_size)
    end_block = math.ceil(io_request.offsetEnd / block_size)
    # size_block = end_block - offset_block

    #  Traiter la demande IO avec la politique de remplacement spécifiée
    policy.on_io(io_request.file, io_request.timestamp, io_request.requestType, offset_block, end_block)

    #  Mettre à jour les listes de données
    policy_hits_data.append(policy.hits)
    policy_misses_data.append(policy.misses)
    policy_evicted_fils_data.append(policy.evicted_file_count)
    policy_evicted_blocks_data.append(policy.evicted_blocks_count)
    policy_time_migration.append(policy.migration_times)
    policy_total_times.append(policy.total_time)
    policy_prefetch_times.append(policy.prefetch_times)
    policy_read_times.append(policy.read_times)
    policy_write_times.append(policy.write_times)
    #  La demande IO est traitée après la fin de la dernière demande IO et le temps de traitement de la demande IO est ajouté au temps de fin de la demande IO
    io_request.execution_end_time = io_request.execution_start_time + policy.ssd_time

    return io_request.execution_end_time, io_request

# Fonction pour simuler une politique avec une file d'attente
def simulate_policy_with_queue1(policy, ios, policy_hits_data, policy_misses_data, policy_evicted_fils_data,
                                policy_evicted_blocks_data, policy_time_migration, policy_total_times,
                                policy_prefetch_times, policy_read_times, policy_write_times):
    """ Simule une politique de remplacement avec une file d'attente de demandes IO et retourne les demandes IO traitées
     et les statistiques de la politique de remplacement simulée.
     Elle traite chaque demande séquentiellement et met à jour les statistiques globales.

    Args:
        policy (Policy): La politique de remplacement à simuler
        ios (list): La liste des demandes IO à simuler
        policy_hits_data (list): La liste pour stocker les hits de la politique
        policy_misses_data (list): La liste pour stocker les misses de la politique
        policy_evicted_fils_data (list): La liste pour stocker les fichiers évictés de la politique
        policy_evicted_blocks_data (list): La liste pour stocker les blocs évictés de la politique
        policy_time_migration (list): La liste pour stocker les temps de migration de la politique
        policy_total_times (list): La liste pour stocker les temps totaux de la politique
        policy_prefetch_times (list): La liste pour stocker les temps de prefetch de la politique
        policy_read_times (list): La liste pour stocker les temps de lecture de la politique
        policy_write_times (list): La liste pour stocker les temps d'écriture de la politique
        Returns:
             list: La liste des demandes IO traitées"""

    #  Initialiser la file d'attente des demandes IO
    io_queue = deque()

    #  Initialiser le temps de fin de la dernière demande IO
    previous_end_time = 0

    #  Initialiser la liste des demandes IO traitées
    processed_io_requests = []

    #  Traiter chaque demande IO séquentiellement et mettre à jour les statistiques globales
    for file, timestamp, requestType, offsetStart, offsetEnd in ios:
        io_request = IORequest(file, timestamp, requestType, offsetStart, offsetEnd)
        io_request.execution_start_time = max(io_request.timestamp, previous_end_time)
        if not io_queue or io_request.execution_start_time > previous_end_time:

            previous_end_time, processed_io = process_io_request_with_queue1(io_request, previous_end_time, policy,
                                                                             policy_hits_data, policy_misses_data,
                                                                             policy_evicted_fils_data,
                                                                             policy_evicted_blocks_data,
                                                                             policy_time_migration, policy_total_times,
                                                                             policy_prefetch_times, policy_read_times,
                                                                             policy_write_times)

            processed_io_requests.append(processed_io)
        else:
            io_queue.append(io_request)

    #  Traiter les demandes IO dans la file d'attente et mettre à jour les statistiques globales
    while io_queue:
        current_io = io_queue.popleft()
        waiting_time = current_io.waiting_time
        previous_end_time, processed_io = process_io_request_with_queue1(current_io, previous_end_time, policy,
                                                                         policy_hits_data, policy_misses_data,
                                                                         policy_evicted_fils_data,
                                                                         policy_evicted_blocks_data,
                                                                         policy_time_migration, policy_total_times,
                                                                         policy_prefetch_times, policy_read_times,
                                                                         policy_write_times)
        # processed_io_requests.append(processed_io)
        policy.total_time += waiting_time
        processed_io_requests.append(processed_io)
    return processed_io_requests


# Fonction pour simuler une politique de remplacement avec une file d'attente et gestion des évictions en arrière-plan
def simulate_policy_with_queue31(policy, ios,
                                 policy_hits_data, policy_misses_data, policy_evicted_fils_data,
                                 policy_evicted_blocks_data, policy_time_migration,
                                 policy_total_times, policy_prefetch_times, policy_read_times, policy_write_times):
    """ Simule une politique de remplacement avec une file d'attente
    Args: policy: la politique de remplacement
          ios: les demandes IO
          policy_hits_data: les hits
          policy_misses_data: les misses
          policy_evicted_fils_data: les fichiers evictes
          policy_evicted_blocks_data: les blocs evictes
          policy_time_migration: le temps de migration
          policy_total_times: le temps total
          policy_prefetch_times: le temps de prefetch
          policy_read_times: le temps de lecture
          policy_write_times: le temps d'ecriture
          Returns: les demandes IO traitees"""

    # Initialisation de la file d'attente des demandes IO
    io_queue = []

    # Initialisation du temps de fin de la dernière demande IO
    previous_end_time = 0

    # Initialisation des compteurs pour les hits, les misses et le temps total
    total_hits = 0
    total_misses = 0
    total_times = 0
    migration_times = 0
    prefetch_times = 0
    read_times = 0
    write_times = 0

    # Traiter chaque demande IO séquentiellement et mettre à jour les statistiques globales
    for file, timestamp, requestType, offsetStart, offsetEnd in ios:
        io_request = IORequest(file, timestamp, requestType, offsetStart, offsetEnd)
        io_request.execution_start_time = max(io_request.timestamp, previous_end_time)
        if not io_queue or io_request.execution_start_time > previous_end_time:
            previous_end_time = process_io_request_with_queue(
                io_request, previous_end_time, policy,
                policy_hits_data, policy_misses_data, policy_evicted_fils_data,
                policy_evicted_blocks_data, policy_time_migration,
                policy_total_times, policy_prefetch_times, policy_read_times, policy_write_times)
            total_times += policy.total_time
            # Traitement de l'éviction et ajout du temps de migration au temps total
            while policy.eviction_queue and (not io_queue or io_request.execution_start_time > previous_end_time):
                policy.actual_evict()
                total_times += policy.ssd_time_evict + policy.hdd_time_evict
        # Ajouter la demande IO à la file d'attente
        else:
            io_queue.append(io_request)

    # Traiter les demandes IO dans la file d'attente et mettre à jour les statistiques globales
    while io_queue:
        current_io = io_queue.pop(0)
        current_io.execution_start_time = max(current_io.timestamp, previous_end_time)
        waiting_time = current_io.waiting_time
        previous_end_time = process_io_request_with_queue(
            current_io, previous_end_time, policy,
            policy_hits_data, policy_misses_data, policy_evicted_fils_data,
            policy_evicted_blocks_data, policy_time_migration,
            policy_total_times, policy_prefetch_times, policy_read_times, policy_write_times)
        prefetch_times += policy.prefetch_times
        read_times += policy.read_times
        write_times += policy.write_times
        total_times += policy.total_time + waiting_time
    # Mise à jour des compteurs
    total_hits += policy.hits
    total_misses += policy.misses
    fils_migration = policy.evicted_file_count
    evicted_blocks_count = policy.evicted_blocks_count

    # Retour des totaux accumulés
    return total_hits, total_misses, total_times, migration_times, prefetch_times, read_times, write_times, fils_migration, evicted_blocks_count


# Fonction de simulation de toutes les politiques
def simulate_all_policies(cache_size_proportions, ios, cache_size, ssd_tier, hdd_tier):
    """ Simuler toutes les politiques avec les paramètres donnés
    :param cache_size_proportions: Liste des proportions de la taille du cache à simuler
    :param ios: Liste des requêtes d'entrée/sortie à simuler
    :param cache_size: Taille du cache à simuler
    :param ssd_tier: Tier SSD à simuler
    :param hdd_tier: Tier HDD à simuler
    :return: Dictionnaire des résultats de simulation
    """
    # Initialiser la liste des résultats
    results = {}

    # Initialiser les politiques  avec les paramètres donnés et les simuler avec les requêtes d'entrée/sortie données
    # pour chaque proportion de taille de cache donnée et stocker les résultats dans la liste des résultats
    for proportion in cache_size_proportions:

        # Calculer la taille du cache en fonction de la proportion donnée
        cache_size_p = round((cache_size * proportion) / 100)

        ssd_tier = Tier("SSD", max_size=cache_size_p, latency=0.0001, read_throughput=2254857830,
                       write_throughput=2147483648)
        #hdd_tier = Tier("HDD", max_size=cache_size, latency=0.01, read_throughput=262144000,
                       # write_throughput=251658240)
        # Initialiser les politiques avec les paramètres donnés
        policies = {

            "ARC": Arc_block_Cache(cache_size_p, alpha, ssd_tier, hdd_tier),
            "CFS-ARC": CFS_ARC(cache_size_p, alpha, ssd_tier, hdd_tier),
            "FG-Arc": FG_ARC(cache_size_p, alpha, ssd_tier, hdd_tier),
            "BFH_ARC_alpa_beta": BFH_ARC_alpha_beta(cache_size_p, alpha, ssd_tier, hdd_tier),
            "BFH_ARC_whithout_alpha_beta": BFH_ARC_whithout_alpha_beta(cache_size_p, alpha, ssd_tier, hdd_tier),
            "BFH_ARC_whith_size": BFH_ARC(cache_size_p, alpha, ssd_tier, hdd_tier),
            "RLT_ARC": RLT_ARC(cache_size_p, alpha, ssd_tier, hdd_tier),
            "Idle_Time_BFH_ARC_policy": Idle_Time_BFH_ARC(cache_size_p, alpha, ssd_tier, hdd_tier),
        }

        # Ajouter la proportion de taille de cache à la liste des résultats
        results[proportion] = {}

        # Simuler les politiques avec les requêtes d'entrée/sortie données et stocker les résultats dans la liste des résultats
        for policy_name, policy in policies.items():
            hits_data, misses_data, total_times, evicted_blocks_data = [], [], [], []
            if policy_name == "Idle_Time_BFH_ARC_policy" or policy_name == "RLT_ARC" or policy_name == "CFS-ARC":
                print("la politique est", policy_name)
                total_hits, total_misses, total_execution_time, migration_times, prefetch_time, read_time, write_time, \
                evicted_files, evicted_blocks = simulate_policy_with_queue31(policy, ios, hits_data, misses_data, [],
                                                                            evicted_blocks_data, [],
                                             total_times, [], [], [])
                results[proportion][policy_name] = {
                    "hit_ratio": (total_hits / (total_hits + total_misses)) * 100,
                    "total_time": sum(total_times),
                    "evicted_blocs": evicted_blocks_data[-1],
                }
            else:
                simulate_policy_with_queue1(policy, ios, hits_data, misses_data, [], evicted_blocks_data, [],
                                        total_times, [], [], [])

                results[proportion][policy_name] = {
                    "hit_ratio": (hits_data[-1] / (hits_data[-1] + misses_data[-1])) * 100,
                    "total_time": sum(total_times),
                    "evicted_blocs": evicted_blocks_data[-1],
                }
    return results


# Fonction pour tracer les résultats
def plot_results(results, metric='evicted_blocs', save_path='graphs'):

    """Tracer les résultats de la simulation
    Args:   results: dict, les résultats de la simulation
            metric: str, la métrique à tracer
            save_path: str, le chemin où sauvegarder les graphes
    """
    proportions = sorted(results.keys())
    policies = sorted(next(iter(results.values())).keys())

    fig, ax = plt.subplots(figsize=(12, 8))
    width = 0.35  # largeur des barres

    # Calculer les positions des barres pour chaque politique
    ind = np.arange(len(proportions))  # les positions x pour les groupes
    offset = width / len(policies)  # décalage pour chaque barre dans un groupe

    for i, policy in enumerate(policies):
        values = [results[proportion][policy][metric] for proportion in proportions]
        ax.bar(ind + i * offset, values, width=offset, label=policy)

    ax.set_xlabel('Proportion de la taille du cache (%)')
    ax.set_title(f'{metric.replace("_", " ").title()} par politique et proportion du cache')
    ax.set_xticks(ind + offset / 2 * (len(policies) - 1))
    ax.set_xticklabels(proportions)
    ax.legend()

    # Différencier les étiquettes Y selon la métrique
    if metric == 'hit_ratio':
        ax.set_ylabel('Taux de succès (%)')
    elif metric == 'total_time':
        ax.set_ylabel('Temps total')
    elif metric == 'evicted_blocs':
        ax.set_ylabel('Nombre de blocs évincés')

    plt.tight_layout()
    #plt.show()
    current_datetime = datetime.datetime.now()

    # Formater la date et l'heure en une chaîne ("2023-04-01_12-30-00")
    formatted_datetime = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")
    # Construire le nom de fichier pour sauvegarder le graphique
    filename = f"{metric}_{formatted_datetime}.png"
    filepath = os.path.join(save_path, filename)
    plt.savefig(filepath)
    plt.close(fig)

# Fonction pour écrire un résumé des résultats dans un fichier
def write_summary_to_file2(log_filename, metadata_filename, cache_size_p, proportion, hits, misses, total_execution_time, total_evicted_files, total_evicted_blocks, IO_count):
    """Écrire un résumé des résultats dans un fichier   
    Args:   log_filename: str, le nom du fichier pour écrire le résumé
            metadata_filename: str, le nom du fichier de métadonnées
            cache_size_p: int, la taille du cache
            proportion: int, la proportion de la taille du cache
            hits: int, le nombre de hits
            misses: int, le nombre de misses
            total_execution_time: float, le temps total d'exécution
            total_evicted_files: int, le nombre total de fichiers évincés
            total_evicted_blocks: int, le nombre total de blocs évincés
            IO_count: int, le nombre total de requêtes d'entrée/sortie
            """
    
    with open(log_filename, 'w') as file:
        file.write(f"Metadata File: {metadata_filename}\n")
        file.write(f"Cache Size: {cache_size_p}\n")
        file.write(f"Cache Proportion: {proportion}%\n")
        file.write(f"Hit Ratio: {((hits / (misses + hits)) * 100):.2f}%\n")
        file.write(f"Hits: {hits}\n")
        file.write(f"Misses: {misses}\n")
        file.write(f"Miss Ratio: {((misses / (misses + hits)) * 100):.2f}%\n")
        file.write(f"Total Execution Time: {total_execution_time}\n")
        file.write(f"Total Evicted Blocks: {total_evicted_blocks}\n")
        file.write(f"Total Evicted Files: {total_evicted_files}\n")
        file.write(f"Total IOs: {IO_count}\n")


# Fonction pour écrire un résumé des résultats dans un fichier
def write_summary_to_file(log_filename, metadata_filename, cache_size_p, hits, misses, total_execution_time,
                          total_eviction_time, total_prefetch_time, total_read_time, total_write_time,
                          total_fils_migrated, total_blocks_migrated, IO_count):
    """Écrire un résumé des résultats dans un fichier
    Args:   log_filename: str, le nom du fichier pour écrire le résumé
            metadata_filename: str, le nom du fichier de métadonnées
            cache_size_p: int, la taille du cache
            hits: int, le nombre de hits
            misses: int, le nombre de misses
            total_execution_time: float, le temps total d'exécution
            total_eviction_time: float, le temps total d'éviction
            total_prefetch_time: float, le temps total de prélecture
            total_read_time: float, le temps total de lecture
            total_write_time: float, le temps total d'écriture
            total_fils_migrated: int, le nombre total de fichiers migrés
            total_blocks_migrated: int, le nombre total de blocs migrés
            IO_count: int, le nombre total de requêtes d'entrée/sortie
    """
    with open(log_filename, 'w') as file:
        file.write(f"Metadata File: {metadata_filename}\n")
        file.write(f"Cache Size: {cache_size_p}\n")
        #file.write(f"La proportion du cache: {proportion}\n")
        file.write(f" ratio Hits: {((hits / (misses + hits)) * 100)}\n")
        file.write(f"Hits: {hits}\n")
        file.write(f"misses: {misses}\n")
        file.write(f" ratioMisses: {((misses / (misses + hits)) * 100)}\n")
        file.write(f"Total Execution Time: {total_execution_time}\n")
        file.write(f"Total Eviction Time: {total_eviction_time}\n")
        file.write(f"Total Prefetch Time: {total_prefetch_time}\n")
        file.write(f"Total Read Time: {total_read_time}\n")
        file.write(f"Total Write Time: {total_write_time}\n")
        file.write(f"Total Evicted Blocks: {total_blocks_migrated}\n")
        file.write(f"Total Evicted Files: {total_fils_migrated}\n")
        file.write(f"Total IOs: {IO_count}\n")

# Fonction pour lire les requêtes d'entrée/sortie à partir d'un fichier
def read_io_requests(filename):
    """Lire les requêtes d'entrée/sortie à partir d'un fichier
    Args:   filename: str, le nom du fichier
        Returns:    un générateur d'objets (filename, timestamp, requestType, offsetStart, offsetEnd)   
    """
    with open(filename, 'r', encoding='utf-8') as f:
        next(f)  # skip header line
        for line in f:
            row = line.strip().split(',')
            filename = row[2]
            timestamp = float(row[0])
            requestType = str(row[1])
            offsetStart = int(row[3])
            offsetEnd = int(row[4])
            yield (files_dict.get(filename, None), timestamp, requestType, offsetStart, offsetEnd)

# Fonction pour lire les requêtes d'entrée/sortie à partir d'un fichier avec une limite
def read_io_requests1(filename, limit_number):
    """Lire les requêtes d'entrée/sortie à partir d'un fichier
    Args:   filename: str, le nom du fichier
            limit_number: int, le nombre limite de requêtes à lire
        Returns:    un générateur d'objets (filename, timestamp, requestType, offsetStart, offsetEnd)
    """
    with open(filename, 'r', encoding='utf-8') as f:
        next(f)  # skip header line
        for i, line in enumerate(f):
            if i >= limit_number:
                break
            row = line.strip().split(',')
            filename = row[2]
            timestamp = float(row[0])
            requestType = str(row[1])
            offsetStart = int(row[3])
            offsetEnd = int(row[4])
            yield (files_dict.get(filename, None), timestamp, requestType, offsetStart, offsetEnd)

# Fonction pour lire les métadonnées des fichiers à partir d'un fichier de métadonnées
def load_file_metadata(metadata_filename, block_size):
    """Lire les métadonnées des fichiers à partir d'un fichier de métadonnées   
    Args:   metadata_filename: str, le nom du fichier de métadonnées
            block_size: int, la taille du bloc
        Returns:    un dictionnaire de fichiers
    """
    
    files_dict = {}
    with open(metadata_filename, 'r', encoding='utf-8') as f:
        next(f)  # Sauter l'en-tête
        for line in f:
            row = line.strip().split(',')
            filename = row[0]
            size = int(row[1])
            firstAccessTime = float(row[2])
            lastAccessTime = float(row[3])
            lifetime = float(row[4])
            files_dict[filename] = File(filename, max(1, math.ceil(size / block_size)), firstAccessTime, lastAccessTime, lifetime)
    return files_dict


if __name__ == "__main__":
    
    # Paramètres du système de stockage 
    block_size = 1024 # Taille du bloc
    cache_size = round(7079388946 / block_size)  # Taille de l'espace adressable du workload
    
    # Création des niveaux de stockage (tiers) 
    ssd_tier = Tier("SSD", max_size=cache_size, latency=0.0001, read_throughput=2254857830, write_throughput=2147483648)
    hdd_tier = Tier("HDD", max_size=cache_size, latency=0.01, read_throughput=262144000, write_throughput=251658240)
    alpha = 1.00
    
    # Paramètres de simulation 
    cache_size_proportions = [0.1, 0.5, 1, 5, 10] # Proportion de la taille du cache
    # cache_size_proportion = [50]
    metadata_filename = "data3/IBMObjectStoreTrace009Part0_metadata.csv" # Fichier de métadonnées
    
    # Chargement des métadonnées des fichiers
    files_dict = load_file_metadata(metadata_filename, block_size)
    
    # Lecture des requêtes d'entrée/sortie à partir d'un fichier 
    #ios = list(read_io_requests("data3/IBMObjectStoreTrace009Part0_request.csv"))
    ios = list(read_io_requests1("data3/IBMObjectStoreTrace009Part0_request.csv", 100))
    #ios = list(read_io_requests("data3/IBMObjectStoreTrace007Part0_request.csv"))
    IO_count = len(ios)

    # Effectuer les simulations
    results = simulate_all_policies(cache_size_proportions, ios, cache_size, ssd_tier, hdd_tier)
    #print(results)
    # Utilisation de la fonction plot_results pour chaque métrique
    # plot_results(results, metric='hit_ratio')
    # plot_results(results, metric='total_time')
    # plot_results(results, metric='evicted_blocs')

    plot_results(results, metric='hit_ratio', save_path='graphes/work7/')
    plot_results(results, metric='total_time', save_path='graphes/work7/')
    plot_results(results, metric='evicted_blocs', save_path='graphes/work7/')