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
        logger.debug(f'Message received: {message}')
        if END_SOURCE_PIPELINE in message:  # Stop the pipeline
            logger.info(f'Open and write into a file. Collect data= {self.collect_data}')
            with open(self.output_path, 'w') as file:
                file.write(str(self.collect_data))
        else:
            self.collect_data.append(message.get('data'))
        return message
