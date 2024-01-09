
from typing import Dict, List, Union

# Abstract file class
class File:
    def __init__(self, name: str, size: int, lifetime: float):
        self.name = name
        self.size = size
        self.lifetime = lifetime
        self.is_eviction_pending = False
    #def __repr__(self) -> str:
     #   return self.name
