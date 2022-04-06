from typing import Dict

from filtering_pipeline.filters.abstract_filter import AbstractFilter
from src import SEQUENCE_ENCODING_TAG, CLUSTER_DICT_TAG
from collections import defaultdict

class FilterClusterSequences(AbstractFilter):
    """
        A filter that clusters the sequences if their encoding is equal
    """

    def __init__(self, conf_filter: Dict = None):
        self.name = 'FilterClusterOrder'
        super().__init__()
        self.cluster_dict = defaultdict(list)

    def process(self,message: object) -> object:
        sequence_idx = message.get('sequence_idx')
        key = message.get(SEQUENCE_ENCODING_TAG)
        self.cluster_dict[key].append(sequence_idx)
        return message

    def last_process(self, message: object) -> object:
        message[CLUSTER_DICT_TAG] = self.cluster_dict
        return message