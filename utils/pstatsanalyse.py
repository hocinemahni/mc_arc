import pstats


p = pstats.Stats('profile_output.pstats2')

# Trier et afficher les statistiques

p.strip_dirs().sort_stats('cumulative').print_stats(10)  # Affiche les 10 principales entrées
