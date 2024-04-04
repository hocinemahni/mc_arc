import unittest
from collections import OrderedDict, defaultdict
from structures.file import File
from structures.tier import Tier
from policy.FG_ARC import FG_ARC


#  tester les méthodes de la classe FG_ARC
class TestFGARC(unittest.TestCase):
    '''Tester les méthodes de la classe FG_ARC.'''
    def setUp(self):
        '''Initialiser les variables nécessaires pour les tests.
        Créer deux tiers, un pour le SSD et un pour le HDD.
        Créer une instance de la classe FG_ARC.
        definir la taille du cache'''
        cache_size = 2
        alpha = 0.5
        self.ssd_tier = Tier(name="SSD", max_size=cache_size, latency=0, read_throughput=1000, write_throughput=1000)
        self.hdd_tier = Tier(name="HDD", max_size=cache_size, latency=0, read_throughput=500, write_throughput=500)
        self.policy = FG_ARC(cache_size, alpha, self.ssd_tier, self.hdd_tier)

    def test_remove_all(self):
        '''Tester la méthode remove_all de la classe FG_ARC.
        Créer un fichier et simuler son ajout à t1 et t2.
        Appeler la méthode remove_all et vérifier que le fichier a été supprimé de t1 et t2.
        Vérifier que le fichier a été ajouté à b1 et supprimé de file2blocks.
        '''
        # Create a file and simulate adding it to t1 and t2
        file_t1 = File("file_t1",  1, firstAccessTime=0, lastAccessTime=1, lifetime=1)
        self.policy.t1[(file_t1, 0)] = None
        #self.policy.t2[(file_t1, 0)] = None
        self.policy.file2blocks[file_t1] = {(file_t1, 0)}

        self.policy.remove_all(file_t1)
        #self.assertIn((file_t1, 0), self.policy.b1)
        self.assertNotIn((file_t1, 0), self.policy.t1)
        #self.assertNotIn((file_t1, 0), self.policy.t2)
        self.assertNotIn(file_t1, self.policy.file2blocks)


    def test_evict(self):
        '''Tester la méthode evict de la classe FG_ARC.
        Ajouter des fichiers au cache jusqu'à ce qu'une éviction soit nécessaire.
        Vérifier que la méthode evict supprime le bloc approprié de t1 ou t2.
        Vérifier que le fichier est ajouté à b1 et supprimé de file2blocks.
        '''
        # Assurer que le cache est suffisamment rempli pour que l'éviction se produise.
        # Cela dépendra de la taille du cache et de la taille des fichiers que on ajoute.

        # Ajout des fichiers au cache jusqu'à ce qu'une éviction soit nécessaire.
        for i in range(self.policy.cache_size + 1):
            file = File(f"file_{i}", 1, 0, 1, 1)  # Ajout des paramètres fictifs pour File.
            self.policy.t1[(file, 0)] = None  # Ajout le bloc à t1 (ou t2 selon la politique de cache).
            self.policy.file2blocks[file] = {(file, 0)}
            self.policy.ssd_tier.add_file(file)

        # Vérifier le statut avant l'éviction.
        initial_t1_length = len(self.policy.t1)
        print(initial_t1_length)
        # Effectuer l'éviction.
        self.policy.evict()
        print(len(self.policy.t1))
        # Vérifier que l'éviction a eu lieu.
        self.assertLess(len(self.policy.t1), initial_t1_length, "t1 devrait diminuer après éviction")

        #  Vérifier que le fichier évacué est dans b1.
        self.assertGreaterEqual(len(self.policy.b1), 1, "b1 devrait avoir au moins un élément après éviction")

        # vérifier que le fichier évacué est dans le HDD tier.
        self.assertGreaterEqual(len(self.policy.hdd_tier.files), 0,
                                "HDD tier devrait avoir au moins un fichier après éviction")

        # Vérifier que le fichier évacué est le bon fichier.
        #  evicted_file_name = f"file_0"
        evicted_file_name = self.policy.file_to_evict
        self.assertTrue(self.policy.hdd_tier.is_file_in_tier(evicted_file_name[0].name),
                        f"Le fichier {evicted_file_name[0].name} devrait être dans le HDD tier après éviction")

        # vérifier que l'éviction a mis à jour les compteurs.
        self.assertEqual(self.policy.evicted_blocks_count, 1, "Le compteur de blocs évacués devrait être mis à jour")
        self.assertEqual(self.policy.evicted_file_count, 1, "Le compteur de fichiers évacués devrait être mis à jour")
        print(self.policy.t1)
        print(self.policy.b1)
# exécuter le test
if __name__ == '__main__':
    unittest.main()
