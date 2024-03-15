
from typing import Dict, List, Union

# Classe File
class File:
    def __init__(self, name: str, size: int, firstAccessTime: float, lastAccessTime: float, lifetime: float, entry_time_ssd: float = None):
        self.name = name  # Nom du fichier
        self.size = size  # Taille du fichier
        self.firstAccessTime = firstAccessTime  # Temps de première accès
        self.lastAccessTime = lastAccessTime  # Temps de dernier accès
        self.lifetime = lifetime  # Durée de vie du fichier
        self.is_eviction_pending = False  # Indicateur pour savoir si le fichier est en attente d'éviction
        self.entry_time_ssd = entry_time_ssd  # Temps d'entrée dans le SSD
        self.vruntime = 0  # Initialiser vruntime avec 0 pour chaque fichier (utilisé pour l'algorithme de remplacement)
    #def __repr__(self) -> str:
     #   return self.name

    # Méthode pour mettre à jour le temps de dernier accès
    def update_entry_time_ssd(self, entry_time: float):
        self.entry_time_ssd = entry_time # Mettre à jour le temps d'entrée dans le SSD