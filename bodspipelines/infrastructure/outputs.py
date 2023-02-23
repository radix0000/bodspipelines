from typing import List, Union
from dataclasses import dataclass

@dataclass
class OutputConsole:
    name: str

    def process(self, item):
        """Print item"""
        print(f"{self.name}: {item}")

class JSONLinesOutput:

    def __post_init__(self):
        pass

    def process(self, item):
        """Print item"""
        print(f"{self.name}: {item}")


@dataclass
class Output:
    """Data output definition class"""
    name: str
    target: Union[OutputConsole]

    def process(self, item):
        self.target.process(item)

@dataclass
class NewOutput:
    """Storage data and output if new definition class"""
    name: str
    storage: Union[ElasticStorage]
    output: Union[OutputConsole]

    def process(self, item, item_type):
        self.storage.process(item, item_type)
        if item:
            self.storage.process(item)
