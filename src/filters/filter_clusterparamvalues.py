from typing import Dict

from scipy.cluster.hierarchy import fclusterdata
from filtering_pipeline.filters.abstract_filter import AbstractFilter
from src import SEQUENCE_ENCODING_TAG, CLUSTER_DICT_TAG
from collections import Counter
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class FilterClusterParamValues(AbstractFilter):
    """
        A filter that clusters the sequences according to the params_array
    """

    def __init__(self, conf_filter: Dict={}):
        super().__init__(conf_filter)
        self.name = 'FilterClusterParamValues'
        self.criterion = conf_filter.get('criterion','inconsistent')
        self.threshold = conf_filter.get('threshold',0.2)
        self.metric = conf_filter.get('metric','cityblock')

    def process(self,message: object) -> object: 
        sequence_list = message.get('list_of_sequences')
        if len(sequence_list) == 1:
            message['weights'] = [1.]
        else:
            params = message.get('params_array')
            clusters = fclusterdata(params, criterion=self.criterion, t=self.threshold, metric=self.metric)
            n_clusters = max(clusters)
            count_elts = Counter(clusters)
            weights = [1./count_elts[idx]/n_clusters for idx in clusters]
            message['weights'] = weights

        return message