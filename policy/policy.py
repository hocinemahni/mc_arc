# Abstract policy class
from typing import List, Dict
from structures.file import File
from structures.tier import Tier
from abc import ABC, abstractclassmethod, abstractmethod

class Policy(ABC):
    def __init__(self, cache_size , alpha, ssd_tier: Tier, hdd_tier: Tier):
        self.tier = ssd_tier
        self.next_tier = hdd_tier
        self.cache_size = cache_size
        self.alpha = alpha
    @abstractmethod
    def on_io(self, file: File, offset: int, size: int):
        pass

    #@abstractmethod
    #def __repr__(self) -> str:
    #    pass

