
from typing import Dict, List, Union

# Abstract file class
class File:
    def __init__(self, name: str, size: int):
        self.name = name
        self.size = size

    def __repr__(self) -> str:
        return self.name
