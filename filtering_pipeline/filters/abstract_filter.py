from abc import ABC, abstractmethod
from typing import Dict

from .. import END_SOURCE_PIPELINE


class AbstractFilter(ABC):

    def __init__(self, conf: Dict = {}):
        self.wrong_ob_cnt: int = 0

    @abstractmethod
    def process(self, message: Dict) -> Dict:
        raise NotImplementedError("process function must be implemented")

    def last_process(self, message: Dict) -> Dict:
        """
            This method implements a last action to do. Can be modify if needed.
        """
        return message


class SourceFilter(AbstractFilter):

    def __init__(self):
        self.gen = self.generator()

    def generator(self) -> Dict:
        pass

    def process(self) -> Dict:
        message = {}
        while True:
            try:
                return next(self.gen)
            except StopIteration:
                return {END_SOURCE_PIPELINE: 'True'}
