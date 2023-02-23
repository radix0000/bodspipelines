from typing import List, Union
from dataclasses import dataclass

from bodspipelines.infrastructure.clients.kinesis_client import KinesisStream

@dataclass
class OutputConsole:
    name: str

    def process(self, item):
        """Print item"""
        print(f"{self.name}: {item}")

#class JSONLinesOutput:
#
#    def __post_init__(self):
#        pass
#
#    def process(self, item):
#        """Print item"""
#        print(f"{self.name}: {item}")

@dataclass
class Output:
    """Data output definition class"""
    name: str
    target: Union[OutputConsole]

    def process(self, item):
        self.target.process(item)


class NewOutput:
    """Storage data and output if new definition class"""
    def __init__(self, storage, output):
        self.storage = storage
        self.output = output

    def process(self, item, item_type):
        self.storage.process(item, item_type)
        if item:
            self.storage.process(item)


class KinesisOutput:
    """Output to Kinesis Stream"""
    def __init__(self, stream_name):
        self.stream_name = stream_name
        self.stream = KinesisStream(self.stream_name)

    def process(self, item):
        self.stream.add_record(item)

    def __del__(self):
        self.stream.finish_write()
