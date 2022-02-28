from typing import Dict
import logging
from ..abstract_filter import SourceFilter


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class SourceList(SourceFilter):
    """
        A basic source that generates a list.
    """

    def __init__(self, conf: Dict = {}):
        super().__init__()
        self.l_data = conf.get('l_data')

    def generator(self) -> object:
        logger.debug('Start generator')
        for elt in self.l_data:
            logger.debug(f'Element : {elt}')
            yield elt 
        logger.info('Stop generator')
