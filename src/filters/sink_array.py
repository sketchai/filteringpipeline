import sys
sys.path.append('src/sketchgraphs/')
sys.path.append('src/filtering-pipeline/')

from typing import Dict
import logging
import numpy as np

from sketchgraphs.data import flat_array
from filtering_pipeline.filters.abstract_filter import AbstractFilter

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class SinkArray(AbstractFilter):
    """
        A sink filter that concatenates all the arrays it is given into a npy file
    """

    def __init__(self, conf_filter: Dict = {}):
        super().__init__()
        self.input_tag: str = conf_filter.get('input_tag')
        self.output_path: str = conf_filter.get('output_path')
        self.collect_data = []
        self.name = 'SinkArray'

    def process(self, message: Dict) -> Dict:
        self.collect_data.append(message.get(self.input_tag))
        return message

    def last_process(self, message: Dict) -> Dict:
        data = np.concatenate(self.collect_data)
        logger.debug(f'Writing {len(data)} weights to {self.output_path}')
        np.save(self.output_path, data, allow_pickle=False)
        return message
