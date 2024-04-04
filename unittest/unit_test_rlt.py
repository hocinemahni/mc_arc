import unittest
from collections import defaultdict
# Importation de la classe RLT_ARC
from policy.RLT_ARC import RLT_ARC
from structures.tier import Tier
from structures.file import File

# Classe MockSSD for Mocking SSD
class MockSSD:
    ''' Classe MockSSD for Mocking SSD
    this class is used to mock the SSD tier.
    '''
    def __init__(self):
        self.files = {}
        self.read_throughput = 500
        self.write_throughput = 500
        self.latency = 0.001

    # method to add file
    def add_file(self, file_name, file):
        self.files[file_name] = file

    # method to remove file
    def remove_file(self, file_name):
        if file_name in self.files:
            del self.files[file_name]

# Classe MockHDD for Mocking HDD
class MockHDD:
    ''' Classe MockHDD for Mocking HDD
    this class is used to mock the HDD tier.
    '''

    def __init__(self):
        self.files = {}
        self.read_throughput = 100
        self.write_throughput = 100
        self.latency = 0.01

    # method to add file
    def add_file(self, file_name, file):
        self.files[file_name] = file

    # method to remove file
    def remove_file(self, file_name):
        if file_name in self.files:
            del self.files[file_name]

# Classe TestRLT_ARCPolicy
class TestRLT_ARCPolicy(unittest.TestCase):
    ''' Classe TestRLT_ARCPolicy
    This class is used to test the RLT_ARC policy.
    '''

    # setUp method is overridden to configure the parameters of the tiers and the RLT_ARC policy.
    def setUp(self):
        ''' Méthode de configuration
        Cette méthode permet de configurer les paramètres des tiers et de la politique RLT_ARC.
        '''
        cache_size = 9 # 9 blocs
        alpha = 0.5
        self.ssd_tier = Tier(name="SSD", max_size=cache_size, latency=0, read_throughput=1000, write_throughput=1000) # tier SSD
        self.hdd_tier = Tier(name="HDD", max_size=cache_size * 10, latency=0, read_throughput=500, write_throughput=500) # tier HDD
        self.policy = RLT_ARC(cache_size, alpha, self.ssd_tier, self.hdd_tier)  # politique RLT_ARC

    # test_cache_operations_and_eviction method is used to test the cache operations and eviction.
    def test_cache_operations_and_eviction(self):
        ''' method to test the cache operations and eviction
        This method is used to test the cache operations and eviction.
        '''
        file1 = File("file1", 2, 1, 4, 3)
        file2 = File("file2", 3, 2, 4, 2)
        file3 = File("file3", 4, 3, 4, 1)

        # Access files to fill up the tier and trigger eviction
        self.policy.on_io(file1, 1, 'GET', 0, 1)
        self.policy.on_io(file2, 2, 'GET', 0, 2)
        self.policy.on_io(file3, 3, 'GET', 0, 3)
        self.policy.on_io(file1, 4, 'GET', 0, 1)

        #  Verify the files in the tier and the eviction functionality
        #  self.assertEqual(self.policy.cache_size, 9, "exact")
        #  self.assertTrue(self.policy.ssd_tier.is_file_in_tier(file2.name), "File2 should be in the SSD tier")
        #  self.assertTrue(self.policy.ssd_tier.is_file_in_tier(file1.name), "File1 should be in the SSD tier")
        #  self.assertTrue(self.policy.ssd_tier.is_file_in_tier(file3.name), "File3 should be in the SSD tier")

        print(self.policy.ssd_tier.files , "files in ssd")
        print(self.policy.hdd_tier.files , "files in hdd")
        #  self.assertIn('file2',  self.policy.ssd_tier.files or self.policy.hdd_tier.files, "File2 whith file.2.size should be in the cache")
        #  self.assertIn('file3',  self.policy.ssd_tier.files or self.policy.hdd_tier.files, "File3 should be in the cache")
        self.policy.evict()
        self.policy.actual_evict()
        print(self.policy.ssd_tier.files, "files in ssd")
        print(self.policy.hdd_tier.files, "files in hdd")
        # Assuming the actual eviction removes the file properly
        self.assertNotIn(file1, self.policy.ssd_cache, "File1 should be evicted from the cache")

        # Verify statistics
        self.assertEqual(self.policy.hits, 1, "Hits should be correctly counted")
        self.assertEqual(self.policy.misses, 6, "Misses should be correctly counted")



if __name__ == '__main__':
    unittest.main()