import json
from typing import Dict
import logging
from filtering_pipeline.filters.abstract_filter import SourceFilter
from src import CLUSTER_DICT_TAG
from src.utils.flat_array import load_dictionary_flat, load_flat_array


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class SourceDict(SourceFilter):
    """
        A source that takes a dict of indexes and a flat array as input
        Outputs clusters of sequences
    """

    def __init__(self, conf: Dict = {}):
        super().__init__()
        indexes = conf.get('indexes')
        data_path = conf.get('data')
        if isinstance(indexes, str):
            with open(indexes,'r') as jsonfile:
                self.d_indexes = json.load(jsonfile)
        elif isinstance(indexes, dict):
            self.d_indexes = indexes

        try:
            logger.info('Load dictionary flat array method')
            self.values = load_dictionary_flat(data_path)['sequences']
        except BaseException:
            logger.info('Load flat array method')
            self.values = load_flat_array(data_path)

    def generator(self) -> object:
        logger.debug('Start generator')
    
        for list_of_indexes in self.d_indexes.values():
            list_of_sequences = [self.values[i] for i in list_of_indexes]
            yield {'list_of_sequences': list_of_sequences}
        logger.info('Stop generator')
