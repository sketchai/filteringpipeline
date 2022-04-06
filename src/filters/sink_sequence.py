from typing import Dict
import logging
import numpy as np

from sketchgraphs.data import flat_array
from filtering_pipeline.filters.abstract_filter import AbstractFilter

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class SinkSequence(AbstractFilter):
    """
        A sink filter that saves all sequences into a flat npy file
    """

    def __init__(self, conf_filter: Dict = {}):
        super().__init__()
        self.output_path: str = conf_filter.get('output_path')
        self.collect_data = []

    def process(self, message: Dict) -> Dict:
        self.collect_data.append(message.get('sequence'))
        return message

    def last_process(self, message: Dict) -> Dict:
        if message.get('sequence') is not None:
            self.collect_data.append(message.get('sequence'))  # Collect last data
        logger.debug(f'Writing {len(self.collect_data)} sequences to {self.output_path}')

        data = flat_array.save_list_flat(self.collect_data)
        np.save(self.output_path, data, allow_pickle=False)

        return message
