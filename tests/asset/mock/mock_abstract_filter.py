from typing import Dict
import logging
from src.filters.abstract_filter import AbstractFilter
from src.filters import KO_FILTER_TAG

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class MockFilter(AbstractFilter):
    """
        A basic filter that change one of the message element.
    """

    def __init__(self, conf_filter: Dict = {}):
        super().__init__()
        self.wrong_ob_cnt: int = 0

    def process(self, message: object) -> object:
        logger.debug(f'message received: {message}')
        if message.get('a'):
            message['a'] += 1
        if message.get('b'):
            self.wrong_ob_cnt += 1
        if message.get('c'):
            message.update({KO_FILTER_TAG: self.name})
        return message
