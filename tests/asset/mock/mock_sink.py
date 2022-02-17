from typing import Dict
import logging

from src.filters.abstract_filter import AbstractFilter
from src.filters import END_SOURCE_PIPELINE

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class MockSinkFilter(AbstractFilter):
    """
        A basic filter that change one of the message element.
    """

    def __init__(self, conf: Dict = {}):
        super().__init__()
        self.output_path: str = conf.get('output_path')
        self.collect_data = []

    def process(self, message: Dict) -> Dict:
        logger.debug(f'-- {self.__class__.__name__}: message received= {message}')
        self.collect_data.append(message.get('count'))
        return message

    def last_process(self, message: Dict) -> Dict:
        logger.debug(f'-- {self.__class__.__name__}: message received= {message}')
        if message.get('count') is not None:
            self.collect_data.append(message.get('count'))  # Collect last data
        logger.debug(f'-- Open and write into a file. Collect data= {self.collect_data}')
        with open(self.output_path, 'w') as file:
            file.write(str(self.collect_data))

        return message
