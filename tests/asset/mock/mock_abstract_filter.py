from typing import Dict
import logging
from src.filters.abstract_filter import AbstractFilter
from src.filters import KO_FILTER_TAG

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class MockFilter(AbstractFilter):
    """
        A basic filter that modify its counter, change a message value and add a KO_FILTER_TAG if the wrong element appears in the message.
    """

    def __init__(self, conf_filter: Dict = {}):
        super().__init__()
        self.l_label: Dict = conf_filter.get('l_label')
        self.name = 'MockFilter'

        self.label_counter: Dict = {}
        for label in self.l_label:
            self.label_counter[label] = 0
            

    def process(self, message: object) -> object:
        logger.debug(f'message received: {message}')

        # Show how to modify class state 
        for label in self.l_label:
            if label in message:
                self.label_counter[label] += 1

        # Show how to modify a message value
        if message.get('a'):
            message['a'] += 1

        # Show how to use the KO_FILTER_TAG
        if message.get('c'):
            message.update({KO_FILTER_TAG: self.name})

        return message

    def last_process(self, message: Dict) -> Dict:
        message = self.process(message)
        message['count'] = self.label_counter
        return message 
