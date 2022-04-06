from typing import Dict
import logging
from filtering_pipeline.filters.abstract_filter import SourceFilter
from ..utils.flat_array import load_flat_array, load_dictionary_flat

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class SourceFromFlatArray(SourceFilter):
    """
        A basic source that generates a list.
    """

    def __init__(self, conf: Dict = {}):
        super().__init__()
        self.file_path = conf.get('file_path')
        self.counter = conf.get('counter_for_test')

    def generator(self) -> object:
        logger.debug('Start generator')
        cpt = 0
        try:
            logger.info('Load dictionary flat array method')
            data = load_dictionary_flat(self.file_path)['sequences']
        except BaseException:
            logger.info('Load flat array method')
            data = load_flat_array(self.file_path)

        for elt in data:
            logger.debug(f'Element : {elt}')
            yield {'sequence': elt, 'sequence_idx': cpt}
            cpt += 1
            if self.counter:
                if cpt > self.counter:
                    break
        logger.info('Stop generator')
