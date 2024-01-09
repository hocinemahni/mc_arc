import matplotlib.pyplot as plt
import numpy as np


tailles_ssd = [0.1, 0.5, 1, 5, 10]  # Tailles SSD
ARC = [99.89372329797862, 99.89372329797862, 99.89372329797862, 99.89372329797862, 99.895583140264]  # Hit ratio pour la politique 1
ARC_file_v1 = [99.89505175675389, 99.89505175675389, 99.89505175675389, 99.89505175675389, 99.895583140264]  # Hit ratio pour la politique 2
ARC_file_v2 = [99.87645333390014, 99.87645333390014, 99.89372329797862, 99.89531744850893, 99.895583140264]  # Hit ratio pour la politique 3
ARC_file_v3= [99.87246795757434, 99.87193657406424, 99.87246795757434, 99.89531744850893, 99.895583140264]  # Hit ratio pour la politique 4

# Configuration des motifs pour chaque politique
motifs = ['o', 'v', 'x', '*']

# Création du graphique à barres
plt.figure(figsize=(12, 8))

# Ajout des barres pour chaque politique avec des motifs
bar_width = 0.2  # Largeur des barres
indices = np.arange(len(tailles_ssd))  # Indices de base pour les tailles SSD

for i, taille in enumerate(tailles_ssd):
    plt.bar(indices[i] - bar_width*1.5, ARC[i], bar_width, 
            label='Politique 1' if i == 0 else "", hatch=motifs[0], edgecolor='black')
    plt.bar(indices[i] - bar_width/2, ARC_file_v1[i], bar_width, 
            label='Politique 2' if i == 0 else "", hatch=motifs[1], edgecolor='black')
    plt.bar(indices[i] + bar_width/2, ARC_file_v2[i], bar_width, 
            label='Politique 3' if i == 0 else "", hatch=motifs[2], edgecolor='black')
    plt.bar(indices[i] + bar_width*1.5, ARC_file_v3[i], bar_width, 
            label='Politique 4' if i == 0 else "", hatch=motifs[3], edgecolor='black')

# Ajouter des légendes et des labels
plt.title('Comparaison des politiques de cache avec motifs')
plt.xlabel('Proportion de la taille SSD')
plt.ylabel('Hit Ratio')
plt.xticks(indices, [str(taille) for taille in tailles_ssd])
plt.legend()

# Afficher le graphique
plt.show()
