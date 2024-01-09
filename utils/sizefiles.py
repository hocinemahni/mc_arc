import csv


def calculer_somme_tailles_fichiers_csv(chemin_fichier):
    somme_tailles = 0
    
    with open(chemin_fichier, 'r') as fichier_csv:
        lecteur_csv = csv.reader(fichier_csv)
        next(lecteur_csv)  # Ignorer l'en-tête du fichier CSV
        
        for ligne in lecteur_csv:
            taille = int(ligne[1])
            somme_tailles += taille
    
    return somme_tailles


chemin_fichier_csv = 'data3/IBMObjectStoreTrace001Part0_metadata.csv'
somme_tailles = calculer_somme_tailles_fichiers_csv(chemin_fichier_csv)
print("La somme des tailles des fichiers CSV est :", somme_tailles)
